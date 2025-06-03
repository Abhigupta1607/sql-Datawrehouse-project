/* 
Purpose :- creating A new database name DATAWAREHOUSE and creating three schema bronze, silver , gold

Precaution-- if database is exist in your system it will drop database 'DATAWAREHOUSE' so use it carefully


*/
-- checking database is exist or not if database is exist then drop it 
IF EXISTS ( select 1 from sys.database where name ='DATAWAREHOUSE')
BEGIN 
  ALTER DATABASE DATAWAREHOUSE SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DATAWAREHOSE
END;

--creating database DATAWAREHOUSE


USE master;

CREATE DATABASE DATAWAREHOUSE;

USE DATAWAREHOUSE;

create Schema bronze;
go
create Schema sliver;
go
create Schema gold;

 
create schema silver;
