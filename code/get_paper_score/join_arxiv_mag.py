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


#
from pyspark.sql import SparkSession



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: parse_arxiv <arxiv_dir> <mag_dir> <output_dir>", file=sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    spark = SparkSession \
        .builder \
        .appName("ParseArxivDataset") \
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     Json record

    arxiv_data = spark.read.json(f"{sys.argv[1]}/*json")

    mag_data = spark.read.json(f"{sys.argv[2]}/*json")

    arxiv_data.printSchema()

    mag_data.printSchema()

    inner_join_res = arxiv_data.join(mag_data, arxiv_data.title == mag_data.title)

    inner_join_res.printSchema()

    print(f"count : {inner_join_res.count()}")

    inner_join_res.write.json(sys.argv[3])

    spark.stop()
