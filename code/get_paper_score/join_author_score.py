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
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: parse_arxiv <explode_res> <author_score> <output_dir>", file=sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    spark = SparkSession \
        .builder \
        .appName("ParseArxivDataset") \
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     Json record

    explode_path = "{}/*json".format(sys.argv[1])

    explode_data = spark.read.json(explode_path)

    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("author_score", DoubleType())
    ])

    author_score = spark.read.csv(sys.argv[2], schema=schema)

    author_score.printSchema()

    inner_join_res = explode_data.join(author_score, explode_data.id == author_score.id).drop(author_score.id)

    inner_join_res.printSchema()

    print("count : {}".format(inner_join_res.count()))

    inner_join_res.write.json(sys.argv[3])

    spark.stop()
