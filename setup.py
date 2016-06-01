from distutils.core import setup

setup(name='forgi',
      version='0.1',
      description='Python NGS tools',
      author='Peter Kerpedjiev',
      author_email='pkerpedjiev@gmail.com',
      url='',
      packages=['negspy'],
      package_data={'negspy': ['data/*/chromInfo.txt']},
      scripts=['scripts/chr_pos_to_genome_pos.py'],

     )
