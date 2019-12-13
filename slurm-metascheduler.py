#!/usr/bin/env python3
# vim: set tabstop=2 softtabstop=2 shiftwidth=2 noexpandtab colorcolumn=81 :
# VIM: let g:pyindent_open_paren=2 g:pyindent_continue=2

import json5
import re
import subprocess
from argparse import ArgumentParser, FileType
from datetime import datetime, timedelta
from math import ceil, floor, inf
from os.path import isdir
from signal import signal, SIGINT
from subprocess import CalledProcessError
from sys import stdin
from time import sleep, strftime

parser = ArgumentParser(
	description='Like GNU parallel, but schedules jobs on a Slurm cluster'
)
parser.add_argument(
	'config_file',
	type=FileType('r'),
	help='Metascheduler configuration file in JSON5 format'
)
parser.add_argument(
	'--poll',
	type=int,
	help='Polling frequency in seconds',
	default=60
)
parser.add_argument(
	'--monitor',
	type=int,
	help='Number of seconds between each status report, 0 for as frequently as possible, -1 for no status reports',
	default=3600
)
parser.add_argument(
	'--retry',
	type=int,
	help='Number of times to retry each command before giving up',
	default=0
)
parser.add_argument(
	'--memory',
	type=float,
	help='Minimum memory required by the most greedy command, in gigabytes',
	default=None
)
parser.add_argument(
	'--timeout',
	type=int,
	help='Number of minutes needed for the slowest command to finish',
	default=0
)
parser.add_argument(
	'--out',
	type=str,
	help='Directory to save each job\'s stdout and stderr in',
	default='.'
)
args = parser.parse_args()
if args.monitor == -1:
	args.monitor = None
else:
	args.monitor = timedelta(seconds=args.monitor)
assert(isdir(args.out))
args.out += '/slurm-%j.out'

queues = json5.load(args.config_file)
for queue in queues:
	if not 'max_jobs' in queue:
		queue['max_jobs'] = 1
	queue['current_job_load'] = 0
	queue['command_completion_times'] = []

class Job:
	def __init__(self, number, commands, queue):
		self.number = number
		self.commands = commands
		self.queue = queue

class Command:
	def __init__(self, line):
		self.line = line
		self.tries = {queue['partition']: 0 for queue in queues}

	def can_go_in(self, queue):
		min_attempt_count = sorted(self.tries.values())[0]
		return self.tries[queue['partition']] <= min_attempt_count

	def total_retries(self):
		retries = -1
		for queue, count in self.tries.items():
			retries += count
		return retries

commands_to_submit = [Command(line.strip()) for line in stdin.readlines()]

total_commands = len(commands_to_submit)
total_finished_commands = 0
total_unfinished_commands = len(commands_to_submit)

min_command_time = inf
max_command_time = 0
total_command_time = 0

submitted_jobs = []

status_last_reported = datetime.min

failed_states = (
	'BOOT_FAIL',
	'CANCELLED',
	'DEADLINE',
	'FAILED',
	'NODE_FAIL',
	'OUT_OF_MEMORY',
	'TIMEOUT'
)

running_states = (
	'COMPLETING',
	'CONFIGURING',
	'RESIZING',
	'RESV_DEL_HOLD',
	'REQUEUE',
	'REQUEUE_FED',
	'REQUEUE_HOLD',
	'REVOKED',
	'RUNNING',
	'SIGNALING',
	'SPECIAL_EXIT',
	'STOPPED',
	'SUSPENDED'
)

def cancel_jobs_and_exit(signal = 0, frame = None):
	for job in submitted_jobs:
		try:
			subprocess.check_call(['scancel', job.number])
		except CalledProcessError:
			pass
	if args.monitor != None:
		print('[' + strftime('%c') + '] Slurm metascheduler aborted')
	exit(-1)

signal(SIGINT, cancel_jobs_and_exit)

def sorted_queues():
	#score each queue by the number of commands it has completed within the last
	#24 hours
	score_denominator = 0
	for queue in queues:
		yesterday = datetime.now() - timedelta(days=1)
		while queue['command_completion_times'] and queue['command_completion_times'][0] < yesterday:
			del queue['command_completion_times'][0]
		queue['score'] = 1 + len(queue['command_completion_times'])
		score_denominator += queue['score']
	#determine each queue's ideal load
	for queue in queues:
		score_fraction = queue['score'] / score_denominator
		queue['ideal_job_load'] = total_unfinished_commands * score_fraction / queue['commands_per_job']
	# If a queue can't accept all the jobs that would go into it if the queue had
	# infinite capacity and never failed, adjust the ideal job loads so that the
	# excess commands are distributed to other queues. For example, if there are
	# 3 queues and 50% of the total commands would go to queue 0, 40% to queue 1,
	# and 10% to queue 2, but queue 0 cannot accept that many jobs, move 80% of
	# queue 0's excess jobs to queue 1 and 20% to queue 2.
	commands_for_other_queues = total_unfinished_commands
	for i, queue in enumerate(queues):
		compatible_commands = 0
		for job in submitted_jobs:
			for command in job.commands:
				if command.can_go_in(queue):
					compatible_commands += 1
		for command in commands_to_submit:
			if command.can_go_in(queue):
				compatible_commands += 1
		compatible_jobs = compatible_commands / queue['commands_per_job']
		excess_jobs = queue['ideal_job_load'] - min(queue['ideal_job_load'], queue['max_jobs'], compatible_jobs)
		queue['ideal_job_load'] -= excess_jobs
		commands_for_other_queues -= queue['ideal_job_load'] * queue['commands_per_job']
		if excess_jobs and commands_for_other_queues > 0:
			excess_commands = excess_jobs * queue['commands_per_job']
			for other_queue in queues[i+1:]:
				excess_jobs = excess_commands / other_queue['commands_per_job']
				ideal_command_load = other_queue['ideal_job_load'] * other_queue['commands_per_job']
				other_queue['ideal_job_load'] += excess_jobs * ideal_command_load / commands_for_other_queues
				#if the other queue's ideal load is now over its max load, it will be
				#fixed in the outer loop
	#return the queues in order of performance score
	return sorted(queues, key=lambda queue: queue['score'], reverse=True)

def format_benchmark(seconds):
	seconds = 0 if seconds == inf else round(seconds)
	days, seconds = divmod(seconds, 24 * 60 * 60)
	hours, seconds = divmod(seconds, 60 * 60)
	minutes, seconds = divmod(seconds, 60)
	ret = ''
	if days:
		ret += str(days) + 'd'
	if days or hours:
		ret += str(hours) + 'h'
	if days or hours or minutes:
		ret += str(minutes) + 'm'
	ret += str(seconds) + 's'
	return ret

if args.monitor != None:
	print(
		'[' + strftime('%c') + '] Slurm metascheduler started on ' +
		str(total_unfinished_commands) + ' commands'
	)
	start_time = datetime.now()

while total_unfinished_commands:
	#submit jobs to queues starting with the highest-performing queues
	for queue in sorted_queues():
		#find commands that can go in this queue, usually commands that haven't
		#tried and failed in this queue before
		commands_for_queue = []
		n_commands = queue['ideal_job_load'] - queue['current_job_load']
		n_commands *= queue['commands_per_job']
		i = 0
		while i < len(commands_to_submit) and len(commands_for_queue) < n_commands:
			command = commands_to_submit[i]
			if command.can_go_in(queue):
				commands_for_queue.append(command)
				del commands_to_submit[i]
			else:
				i += 1
		#fill the queue to its ideal load
		while commands_for_queue:
			job_commands = commands_for_queue[:queue['commands_per_job']]
			del commands_for_queue[:queue['commands_per_job']]
			sbatch = 'sbatch -A ' + queue['account'] + ' -p ' + queue['partition']
			if args.memory:
				mem = ceil(args.memory * len(job_commands) * 1e6)
				sbatch += ' --mem ' + str(mem) + 'K'
			if args.timeout:
				sbatch += ' -t ' + str(args.timeout)
			if 'qos' in queue:
				sbatch += ' --qos ' + queue['qos']
			sbatch += ' -o ' + args.out + ' -e ' + args.out
			sbatch += ' -n 1 --no-requeue << EOF1\n'
			sbatch += '#!/bin/bash\n'
			sbatch += 'parallel << EOF2\n'
			for command in job_commands:
				sbatch += command.line + '\n'
			sbatch += 'EOF2\n'
			sbatch += 'EOF1'
			try:
				output = subprocess.check_output(sbatch, shell=True).decode()
			except CalledProcessError as e:
				#Slurm is flaky. If sbatch fails the first time, try again later.
				print(
					'[' + strftime('%c') + '] WARNING: ' +
					'sbatch failed to submit a job with the commands:'
				)
				for command in job_commands:
					print(command.line)
				if e.stdout == 'sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n':
					commands_to_submit += job_commands
					continue
				else:
					raise e
			job_number = re.match(r'Submitted batch job (\d+)', output).group(1)
			submitted_jobs.append(Job(job_number, job_commands, queue))
			queue['current_job_load'] += 1

	#print a status report periodically
	time_since_last_report = datetime.now() - status_last_reported
	if args.monitor != None and time_since_last_report > args.monitor:
		line = '[' + strftime('%c') + '] Current / ideal loads:'
		for queue in queues:
			line += ' ' + queue['partition'] + ': '
			line += str(queue['current_job_load']) + '/'
			line += str(ceil(queue['ideal_job_load']))
		print(line)
		line = '[' + strftime('%c') + '] Finished ' + str(total_finished_commands)
		line += ' of ' + str(total_commands) + ' commands ('
		line += str(floor(100 * total_finished_commands / total_commands)) + '%)'
		print(line)
		status_last_reported = datetime.now()

	#give the jobs some time to complete
	sleep(args.poll)

	#check on jobs
	i = 0
	while i < len(submitted_jobs):
		job = submitted_jobs[i]
		try:
			job_info = subprocess.check_output([
				'sacct', '-j', job.number, '--noheader', '-o', 'State%20'
			])
		except CalledProcessError as e:
			#Slurm is flaky. If sacct fails to communicate with the job database, try
			#it again later.
			print(
				'[' + strftime('%c') + '] WARNING: ' +
				'sacct failed to determine the state of job ' + job.number
			)
			i += 1
			continue
		#more slurm flakiness: sometimes sacct succeeds but returns bad output
		job_info = job_info.decode()
		job_state = re.search(r'\w+', job_info)
		job_state = job_state.group(0) if job_state else job_info
		if job_state == 'PENDING':
			#move the commands to a faster queue if it makes sense
			preclaimed_slots = len(commands_to_submit)
			for queue in sorted_queues():
				if queue == job.queue:
					i += 1
					break
				effective_job_load = queue['current_job_load']
				effective_job_load += preclaimed_slots / queue['commands_per_job']
				if effective_job_load < queue['ideal_job_load']:
					try:
						subprocess.check_call(['scancel', job.number])
						del submitted_jobs[i]
						job.queue['current_job_load'] -= 1
						commands_to_submit += job.commands
						break
					except CalledProcessError:
						pass
				#this queue is effectively full, so subtract the number of commands
				#that will go to this queue and try the next one
				jobs_for_queue = queue['ideal_job_load'] - queue['current_job_load']
				jobs_for_queue = ceil(jobs_for_queue)
				preclaimed_slots -= jobs_for_queue * queue['commands_per_job']
				preclaimed_slots = max(preclaimed_slots, 0)
		elif job_state in running_states:
			#leave the job alone
			i += 1
		elif job_state in failed_states:
			print(
				'[' + strftime('%c') + '] WARNING: ' +
				'Job ' + job.number + ' failed with state ' + job_state +
				'. Its commands were:'
			)
			for command in job.commands:
				print(command.line)
			#update retry counts
			for j in range(len(job.commands)):
				command = job.commands[j]
				job.commands[j].tries[job.queue['partition']] += 1
				if command.total_retries() >= args.retry:
					print(
						'[' + strftime('%c') + '] ERROR: ' +
						'The following command has failed ' +
						str(1 + command.total_retries()) + ' times:'
					)
					print(command.line)
					del submitted_jobs[i]
					cancel_jobs_and_exit()
			#try again
			del submitted_jobs[i]
			job.queue['current_job_load'] -= 1
			commands_to_submit += job.commands
		elif job_state == 'PREEMPTED':
			#try again
			print(
				'[' + strftime('%c') + '] WARNING: ' +
				'Job ' + job.number + ' was preempted and its commands will be ' +
				'run again:'
			)
			for command in job.commands:
				print(command.line)
			del submitted_jobs[i]
			job.queue['current_job_load'] -= 1
			commands_to_submit += job.commands
		elif job_state == 'COMPLETED':
			#factor the job's running time into the statistics
			try:
				job_info = subprocess.check_output([
					'sacct', '-j', job.number, '--noheader', '-o', 'CPUTimeRAW%20'
				])
			except CalledProcessError as e:
				print(
					'[' + strftime('%c') + '] WARNING: ' +
					'sacct failed to determine the running time of job ' + job.number
				)
				i += 1
				continue
			job_time = int(re.search(r'\w+', job_info.decode()).group(0))
			# Slurm multiplies the job's running time by the node's number of cores
			# (even if some cores sat idle), so compensate for that as well as we
			# can to get an estimate of the time each individual command took.
			command_time = job_time / job.queue['commands_per_job']
			if command_time > max_command_time:
				max_command_time = command_time
			if command_time < min_command_time:
				min_command_time = command_time
			total_command_time += command_time * len(job.commands)
			#count the job as a success
			del submitted_jobs[i]
			job.queue['current_job_load'] -= 1
			job.queue['command_completion_times'] += (
				[datetime.now()] * len(job.commands)
			)
			total_finished_commands += len(job.commands)
			total_unfinished_commands -= len(job.commands)
		else:
			#print a warning but leave the job alone
			print(
				'[' + strftime('%c') + '] WARNING: ' +
				'Job ' + job.number + ' is in the unrecognized state "' + job_state + '"'
			)
			i += 1

if args.monitor != None:
	print(
		'[' + strftime('%c') + '] Slurm metascheduler finished ' +
		str(total_finished_commands) + ' commands successfully'
	)
	if total_finished_commands:
		mean_command_time = total_command_time / total_finished_commands
	else:
		mean_command_time = 0
	print(
		'[' + strftime('%c') + '] Command running times:' +
		' Min: ' + format_benchmark(min_command_time) +
		' Max: ' + format_benchmark(max_command_time) +
		' Mean: ' + format_benchmark(mean_command_time) +
		' Total: ' + format_benchmark(total_command_time)
	)
	wall_clock_time = datetime.now() - start_time
	print(
		'[' + strftime('%c') + '] Wall-clock time: ' +
		format_benchmark(
			wall_clock_time.days * 24 * 60 * 60 +
			wall_clock_time.seconds
		)
	)
