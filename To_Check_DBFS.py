# Databricks notebook source
display(dbutils.fs.ls("dbfs:/mnt/datalake/bronze/"))
display(dbutils.fs.ls("dbfs:/mnt/datalake/silver/"))
display(dbutils.fs.ls("dbfs:/mnt/datalake/gold/"))
