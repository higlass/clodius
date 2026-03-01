import polars as pl
import pytest
from clodius.tiles.gff import parse_gff_to_models

def test_parse_gff_comprehensive():
    """Test parsing both genomic.10k.gff and genomic.gff files"""
    
    for gff_file in ["data/genomic.10k.gff", "data/genomic.gff"]:
        df = pl.read_csv(
            gff_file,
            separator='\t',
            comment_char='#',
            has_header=False,
            new_columns=['seqid', 'source', 'type', 'start', 'end', 'score', 'strand', 'phase', 'attributes'],
            n_rows=5000  # Test subset for performance
        )
        
        genes, transcripts = parse_gff_to_models(df)
        
        # Basic assertions
        assert isinstance(genes, dict)
        assert isinstance(transcripts, dict)
        
        # Should have some genes if there are gene features in the data
        gene_features = df.filter(pl.col('type') == 'gene')
        if len(gene_features) > 0:
            assert len(genes) > 0, f"No genes parsed from {gff_file}"
        
        # Should have transcripts if there are transcript features
        transcript_features = df.filter(pl.col('type').is_in(['mRNA', 'lnc_RNA', 'tRNA', 'rRNA', 'snoRNA']))
        if len(transcript_features) > 0:
            assert len(transcripts) > 0, f"No transcripts parsed from {gff_file}"