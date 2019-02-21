#Makefile

BLASTROOT = /panfs/pan1.be-md.ncbi.nlm.nih.gov/blastprojects/blast_build/c++/
BLAST_INCLUDES = -I$(BLASTROOT)/include -I$(BLASTROOT)/ReleaseMT/inc
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:..

all: blastjni_unit_test

../libblastjni.so: ../blastjni.cpp ../blastjni.hpp
	make -C .. libblastjni.so

blastjni_unit_test: blastjni_unit_test.cpp ../libblastjni.so
	g++ -std=gnu++11 -Wall -O -Iboost_test -I.. $(BLAST_INCLUDES) \
	 -L.. -lblastjni -o $@  $<


check: blastjni_unit_test
	./blastjni_unit_test


clean:
	rm blastjni_unit_test

.PHONY: clean
	 