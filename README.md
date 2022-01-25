# pdx-liftover

The pdx-lifterover is a program to convert the PDCM Finder format mutation data from hg19/GRCh37 to GRCh38.

The program takes the following arguments and must be uppercase:

```
--LIFT - runs the liftover pipeline. Is required to run.
--MUT - toggles mutation data mode. This is the only supported mode. Required to run on a mutation set.
--DIR - pass the UPDOG style folder. For examples the mut.tsv should be in the mut folder of the data provider.
```

For example, the folder structure should be:

```
data_submission_folder
└── mut_folder
    └── data_mut.tsv
```

The liftOver program will then start converting data points. Two files will be output in the `mut_folder`. One file will be the `lift.log` and the 
lifted data will be the input file name appended with `lifted`

The log file will output the index of rows that did not "lift" over succesfully and the row data. Data oftenly does not get lifted over because the row 
does not contain the minimum information neccessary or the genomic position was dropped in GRCh38.

## While the liftOver program takes both the chromosome and sequence position of the data it does not discern between GRCH37/hg19 and GRCh38. ##
## The user of the pipeline must know the genome assembly of the original data. ##
