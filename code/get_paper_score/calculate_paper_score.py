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
from pyspark.sql import functions

#
from pyspark.sql import SparkSession



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: parse_arxiv <join_res_dir> <output_dir>", file=sys.stderr)
        sys.exit(-1)


    # Initialize the spark context.
    spark = SparkSession \
        .builder \
        .appName("ParseArxivDataset") \
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     Json record

    join_res_path = "{}/*json".format(sys.argv[1])

    join_res = spark.read.json(join_res_path)

    join_res.printSchema()

    category_year_score = join_res.withColumn("category", functions.explode(functions.col("categories"))) \
                        .groupBy("category", "year").sum("n_citation")

    print("count : {}".format(category_year_score.count()))

    category_year_score.write.json(sys.argv[2])

    spark.stop()
