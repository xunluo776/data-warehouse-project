/*
Creating database and schema


This script creates a new database named 'DataWarehouse' after checking if it already exists. 
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
within the database: 'bronze', 'silver', and 'gold'. Note that it will drop the original db DataWareHouse if exists
*/

use master;
go
-- checking if the database exists
if exists (select 1 from sys.databases where name = 'DataWareHouse')
begin
	alter database DataWareHouse set SINGLE_USER with rollback immediate;
	drop database DataWareHouse;
	end;
	go


-- for creating the db DataWareHouse
create database DataWareHouse;
go
use DataWareHouse;
go

-- creating schema bronze 
create schema bronze;
go
-- creating schema  silver 
create schema silver;
go
-- creating schema gold
create schema gold;
go
