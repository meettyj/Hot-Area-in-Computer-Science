# Hot-Area-in-Computer-Science

Analyze the next hot area in computer science area using Spark and HDFS. 

How to run the program:

1. Dataset Download:
Arxiv Dataset (800MB): https://arxiv.org/help/bulk_data

Oag Dataset(100GB): https://aminer.org/open-academic-graph

2. Cluster Information:
We use NYU dumbo cluster to run our program. The information of this cluster is list below:
    
    44 compute nodes
    
    2x8-core Intel "Haswell" (c 2014) CPUs
    
    128GB memory
    
    16x2 TB disk for HDFS
    
    10 Gb Ethernet


3. Upload datasets to HDFS
    

4. Design data pipeline and submit jobs to spark.

All the related code is under the ${project_dir}/code folder. 

4.1 The parse_arxiv.py and parse_mag.py are used to filter the fields we need. It takes about 40
minutes to run the parse_mag.py stage

4.2 After the filter stage, we join different datasets together by paper title or author name. 
The join_arxiv_mag.py and join_author_score.py are used to do that

4.3 After that, we used the calculate_paper_score.py to do group by and calculate
the final score. The formulas and methods we used to analysis the data are described in our report

 

