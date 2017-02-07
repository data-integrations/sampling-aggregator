# Sampling Aggregate


Description
-----------
Sampling a large dataset flowing through this plugin to pull random records. Supports two types of sampling
i.e, Systematic Sampling and Reservoir Sampling.


Properties
----------
**sampleSize:** The number of records that needs to be sampled from the input records. Either of 'samplePercentage'
or 'sampleSize' should be specified for this plugin.

**samplePercentage:** The percentage of records that needs to be sampled from the input records. Either of
'samplePercentage' or 'sampleSize' should be specified for this plugin.

**samplingType:** Type of the Sampling algorithm that should to be used to sample the data. This can be either
Systematic or Reservoir.

**overSamplingPercentage:** The percentage of additional records that should be included in addition to the input
sample size to account for oversampling. Required for Systematic Sampling.

**random:** Random float value between 0 and 1 to be used in Systematic Sampling. If not provided, plugin will
internally generate random value.

**totalRecords:** Total number of input records for Systematic Sampling.


Example
-------

This example read data from some stream and sort them alphabetically using a OrderBy plugin and uses
Systematic Sampling to sample the input records considering the sample size and oversampling percenatage mentioned in
the inputs below:

    {
        "name": "Sampling",
        "type": "batchaggregator"
        "properties": {
            "sampleSize": "3",
            "samplingType": "Systematic",
            "overSamplingPercentage": "20",
            "totalRecords": "10"
        }
    }

If the aggregator receives as an input record:

     +=====================================+
     | id  | name   | salary | occupation  |
     +=====================================+
     | 1   | John   | 1000   | Artist      |
     | 2   | Kelly  | 2000   | Singer      |
     | 3   | Kiara  | 3000   | Scientist   |
     | 4   | Phoebe | 2500   | Farmer      |
     | 5   | Mike   | 4000   | Baker       |
     | 6   | Avril  | 4300   | Banker      |
     | 7   | Miley  | 8700   | Actress     |
     | 8   | Katy   | 6500   | Chef        |
     | 9   | Seth   | 2300   | Miner       |
     | 10  | Ben    | 9800   | Director    |
     +=====================================+

After, applying Systematic sampling, plugin will emit 4 random records considering the sample size and over-sampling
percentage provided in the inputs.





