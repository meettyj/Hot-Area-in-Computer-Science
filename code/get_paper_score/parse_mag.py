#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import print_function

import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: parse_mag <intput_dir> <output_dir>", file=sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    spark = SparkSession \
        .builder \
        .appName("ParseArxivDataset") \
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     Json record

    path = "{}/*txt".format(sys.argv[1])

    df = spark.read.json(path)

    #df.printSchema()

    parse_record = df.select(df['title'], df['year'], df['n_citation'])

    parse_record.printSchema()

    parse_record.write.json(sys.argv[2])

    spark.stop()
