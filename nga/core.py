#! /usr/bin/python3

"""Module containing common functions used by multiple components of the nga package."""

from sys import stdout, stderr

def stdoutWF(content, min_verbosity=1, verbosity=1):
	'''Write to stdout and immediately flush.'''

	if verbosity >= min_verbosity:
		stdout.write(content)
		stdout.flush()

def stderrWF(content):
	'''Write to stderr and immediately flush.'''

	stderr.write(content)
	stderr.flush()
