import polars as pl
import pytest
from clodius.models.gff_models import *
from clodius.tiles.gff import parse_gff_to_models

def test_load_and_parse_gff_positions():
    """Test loading positions 879 to 5039 for contig NC_004353.4 from genomic_10k.gff"""
    
    # Load the GFF file
    gff_file = "data/genomic.10k.gff"
    
    # Read GFF file, filtering for the specified contig and position range
    df = pl.read_csv(
        gff_file,
        separator='\t',
        comment_char='#',
        has_header=False,
        new_columns=['seqid', 'source', 'type', 'start', 'end', 'score', 'strand', 'phase', 'attributes']
    )
    
    # Filter for NC_004353.4 contig and position range 879-5039 (JYalpha gene)
    filtered_df = df.filter(
        (pl.col('seqid') == 'NC_004353.4') & 
        (pl.col('start') >= 879) & 
        (pl.col('end') <= 5039)
    )
    
    assert len(filtered_df) > 0, "No entries found in the specified range"
    
    # Parse the filtered dataframe into models
    genes, transcripts = parse_gff_to_models(filtered_df)
    
    # Assertions to verify parsing worked correctly
    assert len(genes) > 0, "No genes were parsed"
    assert len(transcripts) > 0, "No transcripts were parsed"
    
    # Check specific gene exists (JYalpha gene should be in this range)
    jyalpha_gene = None
    for gene_model in genes.values():
        if 'JYalpha' in gene_model.get('gene', {}).get('attributes', {}).get('Name', ''):
            jyalpha_gene = gene_model
            break
    
    assert jyalpha_gene is not None, "JYalpha gene not found in parsed results"
    assert jyalpha_gene['gene']['start'] == 879, "JYalpha gene start position incorrect"
    assert jyalpha_gene['gene']['end'] == 5039, "JYalpha gene end position incorrect"
    assert len(jyalpha_gene['transcripts']) > 0, "JYalpha gene should have transcripts"
    
    # Check that transcripts have exons
    transcript_with_exons = None
    for transcript in transcripts.values():
        if len(transcript.get('exons', [])) > 0:
            transcript_with_exons = transcript
            break
    
    assert transcript_with_exons is not None, "No transcripts with exons found"
    assert len(transcript_with_exons['exons']) >= 1, "Transcript should have at least one exon"
    
    # Check that mRNA transcripts have CDS
    mrna_with_cds = None
    for transcript in transcripts.values():
        if len(transcript.get('cds', [])) > 0:
            mrna_with_cds = transcript
            break
    
    assert mrna_with_cds is not None, "No mRNA transcripts with CDS found"
    assert len(mrna_with_cds['cds']) >= 1, "mRNA transcript should have at least one CDS"
    
    return genes, transcripts