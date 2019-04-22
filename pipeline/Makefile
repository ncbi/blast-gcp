# Makefile for 
# Author: Christiam Camacho (camacho@ncbi.nlm.nih.gov)
# Created: Mon 22 Apr 2019 05:59:04 PM EDT

SHELL=/bin/bash
.PHONY: all clean distclean check

all:
	echo all

#########################################################################
# Python support
VENV=env

check_python: ${VENV}
	source ${VENV}/bin/activate && \
	for f in $(wildcard *.py); do python -m py_compile $$f ; done && \
	python3 -m unittest $(subst .py,,$(filter-out setup.py, $(wildcard *.py))) && \
	python3 -m unittest discover -s tests

${VENV}: requirements.txt
	[ -d ${VENV} ] || virtualenv -p python3 $@
	source ${VENV}/bin/activate && pip install -r $^

.PHONY: parse-logs
parse-logs: ${VENV}
	source ${VENV}/bin/activate && ./parse-search-logs.py -h

clean:
	find . -name __pycache__ | xargs ${RM} -fr

distclean: clean
	${RM} -r ${VENV}
