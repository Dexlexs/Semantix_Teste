from Functions

#-----------------------------------------------------------------------------------------------#
def main(rdd):
  distinctHosts(rdd)
  count_response_404(rdd)
  endpoints_top(rdd)
  response_404_daily_count(rdd)
  total_byte_count(rdd)


#-----------------------------------------------------------------------------------------------#
main(July_Logs)
main(August_Logs)