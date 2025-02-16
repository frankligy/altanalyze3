import pytest
import pathlib
from altanalyze3.utilities.parser import ArgsParser
from altanalyze3.utilities.helpers import get_md5_sum
from altanalyze3.components.intron_count.main import get_jobs


DATA_FOLDER = pathlib.Path(__file__).resolve().parents[1].joinpath("data")


@pytest.mark.parametrize(
    "args, control_chr",
    [
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "chr1", "chr2", "chr3"
            ],
            [
                "chr1", "chr2", "chr3"
            ]
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "1", "2", "3"
            ],
            [
                "chr1", "chr2", "chr3"
            ]
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "chr1", "chr2", "chr3", "chr99"
            ],
            [
                "chr1", "chr2", "chr3"
            ]
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "chr99"
            ],
            []
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz"
            ],
            [
                "chr1", "chr10", "chr11", "chr12", "chr13", "chr14", "chr15",
                "chr16", "chr17", "chr18", "chr19", "chr2", "chr20", "chr21",
                "chr22", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
                "chrX", "chrY"
            ]
        )
    ]
)
def test_get_jobs(monkeypatch, args, control_chr):
    monkeypatch.chdir(DATA_FOLDER)
    args = ArgsParser(args)
    calculated_chr = [job.contig for job in get_jobs(args)]
    assert sorted(calculated_chr) == sorted(control_chr)


@pytest.mark.parametrize(
    "args, control_md5_sum",
    [
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "chr1", "chr2", "chr3"
            ],
            "d41d8cd98f00b204e9800998ecf8427e"
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "1", "2", "3"
            ],
            "d41d8cd98f00b204e9800998ecf8427e"
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "chr1", "chr2", "chr3", "chr99"
            ],
            "d41d8cd98f00b204e9800998ecf8427e"
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz",
                "--chr", "chr99"
            ],
            "d41d8cd98f00b204e9800998ecf8427e"
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe.bam",
                "--ref", "hg19_ref_introns.bed.gz"
            ],
            "7d8c35eb9ce89bcfe0a487b00ef490c5"
        ),
        (
            [
                "intcount",
                "--bam", "hg19_pe_not_indexed.bam",
                "--ref", "hg19_ref_introns.bed.gz"
            ],
            "7d8c35eb9ce89bcfe0a487b00ef490c5"
        )
    ]
)
def test_count_introns(monkeypatch, args, control_md5_sum):             # tests all function from intron_count.main file
    monkeypatch.chdir(DATA_FOLDER)
    args = ArgsParser(args)
    args.func(args)
    calculated_md5_sum = get_md5_sum(args.output.with_suffix(".bed"))
    args.output.with_suffix(".bed").unlink()
    assert calculated_md5_sum == control_md5_sum