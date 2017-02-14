--creation of outlinks  table

create table outlinks as select paperID,count(refID) as outlink from linkgraph group by paperID;

--creation of inlinks table

create table inlinks as select refID as referenceID,count(paperID) as inlink from linkgraph group by refID;

--creation of count table
create table count as select count(distinct paperID) as count from linkgraph ;

--step:1: creating a table with inlinks and outlinks of a paperID

create table links as select o.paperID as paperID,COALESCE(o.outlink,CAST(0 AS BIGINT)) as outlink,COALESCE(i.inlink,CAST(0 AS BIGINT)) as inlink from outlinks o left outer join inlinks i on (o.paperID=i.referenceID);

--step:2(creating a table with inlinks and outlinks of referenceid of a paper)

create table links_referenceID as select d.paperID as paperID,d.refID as referenceID,COALESCE(l.outlink,CAST(0 AS BIGINT)) as outlinksofreferenceID,COALESCE(l.inlink,CAST(0 AS BIGINT)) as inlinksofreferenceID from linkgraph d left outer JOIN links l on(d.refID=l.paperID);


--Step:3:(total in and out links of the referenced referenceID of a paperID)

create table weightedlinks as select paperID,sum(outlinksofreferenceID) as totalrefoutlinks,sum(inlinksofreferenceID) as totalrefinlinks from links_referenceID group by paperID;


--Step:4:(total in and outlinks of referenceID of a paper and in and out links of referenceID's)

create table inoutlinks as select w.paperID as paperID,l.referenceID as referenceID,COALESCE(w.totalrefoutlinks,CAST(0 AS BIGINT)) as totaloutlinks,COALESCE(w.totalrefinlinks,CAST(0 AS BIGINT)) as totalinlinks,l.outlinksofreferenceID as outlink,l.inlinksofreferenceID as inlink from weightedlinks w left outer join links_referenceID l on(w.paperID=l.paperID);


--Step:5(calculating win and wout)

create table winout as select p.paperID,p.referenceID,p.inlink/p.totalinlinks as win,p.outlink/p.totaloutlinks as wout,c.count as count from inoutlinks p cross join count c on(1=1) where p.totalinlinks !=0 and p.totaloutlinks !=0 ;



--Iteration:1


create table pagerank_Iteration1 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank1,count  from
(select  p.paperID,COALESCE(sum((1/p1.count)*p1.win*p1.wout),CAST(0 AS BIGINT))as prank,COALESCE(p1.count,CAST(0 AS BIGINT)) as count from links p left outer join winout p1 ON (p.paperID =p1.referenceID) group by p1.count,p.paperID)a where count!=0;


--Iteration:2


create table pagerank_Iteration2 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank2,count  from
(select  p.paperID,COALESCE(sum((i.rank1)*p1.win*p1.wout),CAST(0 AS BIGINT)) as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration1 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;



--Iteration:3


create table pagerank_Iteration3 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank3,count  from
(select  p.paperID,COALESCE(sum((i.rank2)*p1.win*p1.wout),CAST(0 AS BIGINT)) as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration2 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;


--Iteration:4


create table pagerank_Iteration4 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank4,count  from
(select  p.paperID,COALESCE(sum((i.rank3)*p1.win*p1.wout),CAST(0 AS BIGINT)) as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration3 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;


--Iteration:5


create table pagerank_Iteration5 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank5,count  from
(select  p.paperID,COALESCE(sum((i.rank4)*p1.win*p1.wout),CAST(0 AS BIGINT)) as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration4 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;

--Iteration:6

create table pagerank_Iteration6 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank6,count  from
(select  p.paperID,COALESCE(sum((i.rank5)*p1.win*p1.wout),CAST(0 AS BIGINT)) as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration5 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;

--Iteration:7

create table pagerank_Iteration7 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank7,count  from
(select  p.paperID,COALESCE(sum((i.rank6)*p1.win*p1.wout),CAST(0 AS BIGINT)) as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration6 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;

--Iteration:8

create table pagerank_Iteration8 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank8,count  from
(select  p.paperID,COALESCE(sum((i.rank7)*p1.win*p1.wout),CAST(0 AS BIGINT)) as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration7 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;

--Iteration:9

create table pagerank_Iteration9 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank9,count  from
(select  p.paperID,COALESCE(sum((i.rank8)*p1.win*p1.wout),CAST(0 AS BIGINT))as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration8 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;

--Iteration:10


create table pagerank_Iteration10 as
select paperID,((1-0.85)/count)+(0.85)*prank as rank10,count  from
(select  p.paperID,COALESCE(sum((i.rank9)*p1.win*p1.wout),CAST(0 AS BIGINT))as prank,p1.count from links p  left outer join winout p1 ON (p.paperID =p1.referenceID) JOIN pagerank_Iteration9 i ON(i.paperID=p1.paperID) group by p1.count,p.paperID)a where count!=0;



--Final Step:


insert overwrite local directory 'Downloads/Project/paper_rank'
row format delimited
fields terminated by '\t'
select db.pagetitle,COALESCE(i.inlink,CAST(0 AS BIGINT)) AS inlink,r.rank10 as rank from pagerank_Iteration10 r JOIN dblpnetwork db ON(db.paperID=r.paperID) LEFT OUTER JOIN inlinks i ON(db.paperID=i.referenceID) order by rank desc limit 10;






























