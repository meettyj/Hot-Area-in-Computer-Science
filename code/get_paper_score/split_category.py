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
import pyspark.sql.functions as f

#
from pyspark.sql import SparkSession



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: parse_arxiv <input_dir> <output_dir>", file=sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    spark = SparkSession \
        .builder \
        .appName("ParseArxivDataset") \
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     Json record

    input_path = "{}/*json".format(sys.argv[1])

    input = spark.read.json(input_path)

    input.select(
        "year", "paper_score",
        f.posexplode(f.split("category", " ")).alias("pos", "cate")
    ) \
        .groupBy("cate", "year").sum("paper_score") \
        .withColumnRenamed("sum(paper_score)", "paper_score")

    input.printSchema()


    input.write.json(sys.argv[2])

    spark.stop()
