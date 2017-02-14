--Creation of database
create database project;

use project;


--Creation of DBLP network table
create table dblpnetwork(paperID bigint,pageTitle String ,referenceID array<bigint>)
row format delimited
fields terminated by '\t'
collection items terminated by ',';

--Loading the input data from eclipse to dblpnetwork table

load data local inpath '/home/administrator/Downloads/Project/ProjectReportRelated/part-r-00000' into table dblpnetwork;

--Creation of  link graph table

create table linkgraph as
select paperID,refID from dblpnetwork 
LATERAL VIEW EXPLODE (referenceID) ref as refID;

--Storing the link graph table into a local directory

insert overwrite local directory 'Downloads/Project/linkgraph' 
row format delimited
fields terminated by ','
select * from linkgraph;

