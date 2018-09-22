To run the spark jobs written in scala:
1.copy Vasanthi_Mudunuri_Program_1.sbt to the present working directory
2.Create path in present working directory as src/main/scala/ and copy Vasanthi_Mudunuri_Program_1.scala,
Vasanthi_Mudunuri_Program_2.scala,Vasanthi_Mudunuri_Program_3.scala,Vasanthi_Mudunuri_Program_4.scala,Vasanthi_Mudunuri_Program_5.scala programs into it
3.From the present working directory type : sbt package
4.After succesfull completion this creates project and target folders under present working directory
5.The target folder contains scala-2.10 directory which contains the jar file vasanthi_mudunuri_program_2.10-1.0.jar 
6.Now give the command below as specified for each program to run the spark job.

Note: here both the input and output paths are in hdfs

Program 1:

spark-submit --class Vasanthi_Mudunuri_Program_1 target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar /CS5433/PA2/movies.dat Average.r Action /vmudunu/program1

Arguments: Inputpath,RScriptPath,Genre,Outputpath

Program 2:

spark-submit --class Vasanthi_Mudunuri_Program_2 target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar /CS5433/PA2/movies.dat /CS5433/PA2/ratings.dat /vmudunu/program2

Arguments: Inputpath1,Inputpath2,Outputpath

Program 3:

spark-submit --class Vasanthi_Mudunuri_Program_3 target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar /CS5433/PA3/ACS/ACS_15_5YR_S0101_with_ann.csv /CS5433/PA3/CFS/CFS_2012_00A01_with_ann.csv /vmudunu/program3

Arguments: Inputpath1,Inputpath2,Outputpath
Output is generated as a sequence file which can be viewed by the following command:
hdfs dfs -text /vmudunu/program3/

Program 4:

spark-submit --class Vasanthi_Mudunuri_Program_4 target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar /TwitterData/ProcessedRetweet.txt /vmudunu/program4

Arguments: Inputpath,Outputpath

In outputpath, files with respect to each modules will be created to check the outputs

Program 5:

Batch interval for this job is 4 seconds

Create two sessions in spark and run the following command in one session

spark-submit --class Vasanthi_Mudunuri_Program_5 target/scala-2.10/vasanthi_mudunuri_program_2.10-1.0.jar 10004 /vmudunu/program5
Arguments: Port,Outputpath

In another session, run the following command
Command to connect to port: nc -lk portnumber

Enter data after connecting line wise words and to terminate press ctrl+c, After this terminate streaming job by pressing ctrl+c
output is also dispalyed in console with respect to batch interval
