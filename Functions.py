from pyspark import SparkConf, SparkContext
import os

Conf = SparkConf().setAppName("Desafio_Semantix")
#SC  = SparkContext(Conf)

July_Logs = sc.textFile("/home/alexandre/Downloads/NASA_access_log_Jul95")
#July_Logs = sc.textFile("/FileStore/tables/NASA_access_log_Jul95")
July_Logs.setName("NASA_July_Logs")
July_Logs = July_Logs.cache()


August_Logs = sc.textFile("/home/alexandre/Downloads/NASA_access_log_Aug95")
#August_Logs = sc.textFile("/FileStore/tables/NASA_access_log_Aug95")
August_Logs.setName("NASA_August_Logs")
August_Logs = August_Logs.cache()


#-----------------------------------------------------------------------------------------------#
#1. Número de hosts únicos.
def distinctHosts(rdd):
  hosts = rdd.flatMap(lambda line: line.split(' ')[0]).distinct().count()
  print("Distinct hosts on {name}: {value}".format(name=rdd.name(), value=hosts))


#-----------------------------------------------------------------------------------------------#
#2. O total de erros 404.
#Função de tratamento de arquivo
def error_code_404(line):
  try:
    code = line.split(' ')[-2]
    if code == '404':
      return True
    except:
      pass
    return False

#Função que retorna todos os Responses 404
def response_404(rdd):
  response_404 = rdd.filter(error_code_404).cache()
  return response_404

#Função que conta todos os Responses 404
def count_response_404(rdd):
  count = response_404(rdd).count()
  print("Total of 404 errors in {name} : {value}".format(name=rdd.name(),value=count))

  
#-----------------------------------------------------------------------------------------------#
#Os 5 URLs que mais causaram erro 404
def endpoints_top(rdd, num):
  print("\nTop 5 most frequent 404 endpoints of {name}:".format(name=rdd.name()))
  rdd = response_404(rdd)
  endpoints = rdd.map(lambda line: line.split('"')[1].split(' ')[1])
  counts = endpoints.map(lambda endpoint: (endpoint, 1)).reduceByKey(add)
  top = counts.sortBy(lambda pair: -pair[1]).take(num)
    
  for endpoint, count in top:
    print("Endpoint: {endpoint}, Quantity: {count}".format(endpoint=endpoint, count=count))

#-----------------------------------------------------------------------------------------------#
#Quantidade de erros 404 por dia
def response_404_daily_count(rdd):
  print("\n404 errors per day of {name}:".format(name=rdd.name()))
  rdd = response_404(rdd)
  date = rdd.map(lambda line: line.split('[')[1].split(':')[0])
  counts = date.map(lambda day: (day, 1)).reduceByKey(add)
  sorts = counts.sortBy(lambda pair: -pair[1]).collect()
  
  for day, count in sorts:
    print("Date: {day}, Quantity: {count}".format(day=day, count=count))

#-----------------------------------------------------------------------------------------------#
#O Total de bytes retornados count
def total_byte_count(rdd):
  only_bytes = rdd.flatMap(lambda line: line.split(' ')[-1]).map(lambda value: (1, value)).reduceByKey(add)
  count_bytes = only_bytes.values().sum()
  print(count_bytes)