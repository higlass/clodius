FROM continuumio/miniconda:4.1.11

# NOTE: Versions have been pinned everywhere. These are the latest versions at the moment, because
# I want to ensure predictable behavior, but I don't know of any problems with higher versions:
# It would be good to update these over time.

# TODO: confirm that gcc is still necessary.
RUN apt-get update && apt-get install -y \
        gcc=4:4.9.2-2 \
    && rm -rf /var/lib/apt/lists/*

RUN conda install --yes --channel bioconda bedtools=2.26.0
