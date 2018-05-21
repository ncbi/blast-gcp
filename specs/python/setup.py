from setuptools import setup

setup(name='blastspecs-py',
      description='BLAST specs',
      version='0.0.2',
      author='Peter Meric',
      author_email='meric@ncbi.nlm.nih.gov',
      url='https://blast.ncbi.nlm.nih.gov',
      packages=['ncbi.blast.formatter', 'ncbi.blast.blast_request'],
     )

