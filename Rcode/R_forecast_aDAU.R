library(tidyverse)
library(readr)
library(prophet)
library(tidyquant)
library(forecast)
library(timetk)
library(googlesheets)
library(zoo)
library(lubridate)
source("R_source_prophet.R")
##################################################################################################################  
## To grab the historical data ----
##################################################################################################################  
# which google sheets do you have access to?
# may ask you to authenticate in a browser!
gs_ls()

# get the google sheet
adau <- gs_title("2019 KR _ WIP")

# list worksheets in this google sheet
gs_ws_ls(adau)

# download one of the sheets using gs_read()
adau_daily <- gs_read(ss=adau, ws ="aDAU on Desktop")
# convert to tibble
adau_daily <- as.tibble(adau_daily) %>%
  mutate(Date = lubridate::mdy(Date)) 

# save the legacy forecast daily into csv file
file_path = "./"
file_name = "kr2019_forecast_adau_uri_"

# prepare data for Prophet model
hist_data <- adau_daily %>% 
  select(Date, adau) %>% 
  filter(!is.na(adau)) %>%
  rename(y = adau, ds = Date) %>%
  filter(ds >= "2017-07-01")

# get several date points 
first_history_date <- min(hist_data$ds)
last_history_date <- max(hist_data$ds)
last_day_in_forecast <- "2019-12-31" # by default we will predict toward end of 2019

# create holiday dates (in our case the end of year and begin of year)
year_in_dat <- seq(from=year(first_history_date), to=year(last_day_in_forecast), by = 1)
holidays <- make_holiday_list(year_in_dat)

###########################################################################
# run automatic model selection over several tuning parameters for "linear" growth and "logistic" growth separately, and save the best parameters settings
best_parameters_linear <- find_best_model_params_few(hist_data, holidays, show_best_param_results = TRUE, growth = c("linear"))

# # option 1: simply choose the set of parameters with the smallest MAPE
# best_parameter <- best_parameters_linear$best_param[1,]

# option 2: among the parameter set that with MAPE less than 1.1*min(MAPE), choose the one with the highest value of changepoint_prior_scale so that we can make the trend more flexible while not overfitting
best_parameter <- best_parameters_linear$all_param %>% filter(MAPE <= 1.1* min(MAPE)) %>% arrange(desc(changepoint_prior_scale), MAPE) %>% filter(row_number()==1)
###########################################################################

# actually forecasting based on the best model we have with point estimate and CI list
predict_data <- hist_data
# to save the forecast results
rst <- NA # store the final forecast data for all best parameters
ci.rst <- NA # store the ci by the end of 2018/2019 for all best parameters

if (best_parameter$growth == 'logistic') {predict_data$cap <- best_parameter$capacity}

prophet_model <- prophet(predict_data, # sometime it will throw warning: In .local(object, ...) : non-zero return code in optimizing,
                           # meaning Optimization has difficulty to terminated normally (need to handle this case)
                           growth = best_parameter$growth, 
                           holidays = holidays,
                           n.changepoints = best_parameter$n_changepoints, 
                           seasonality.prior.scale = best_parameter$seasonality_prior_scale, 
                           changepoint.prior.scale = best_parameter$changepoint_prior_scale,
                           holidays.prior.scale = best_parameter$holidays_prior_scale,
                           yearly.seasonality = TRUE,  # yearly seasonal component using Fourier series
                           weekly.seasonality = TRUE
)
# to get the forecasting horizon (since last historical date to the pre-set forecast_end_date)
forecast_period <- ceiling(as.double(difftime(last_day_in_forecast, last_history_date, units="days")))
future <- make_future_dataframe(prophet_model, freq = "day", periods = forecast_period, include_history = TRUE)
future <- future %>% mutate(ds = ymd(ds)) %>% filter(ds<= last_day_in_forecast) # only select the date range that we want 
if (best_parameter$growth == 'logistic') {future$cap <- best_parameter$capacity}
forecast <- predict(prophet_model, future) 

# calculate the CI for 91d MA for the rest days of 2019
start_date <- last_history_date + days(1)
end_date <- "2019-12-31"
ci.prop <- c(0.9)
ci.rst <- ci.gen.calculation(prophet_model, start_date, end_date, ci.prop, file_path, file_name, best_parameter)
write.csv(ci.rst, file=paste0(file_path, file_name, "upto_", last_history_date, "_ci_", best_parameter$growth, ".csv"))
  
# plot the component effects for each best parameters, and save in png  
temp.png <- prophet_plot_components(prophet_model, forecast)
png(paste0(file_path, file_name, "upto_", last_history_date, "_", best_parameter$growth,  "_bestparam_components_all.png"))
gridExtra::grid.arrange(temp.png[[1]], temp.png[[2]], temp.png[[3]], temp.png[[4]], nrow=4) # plot trend, holiday, weekly, and yearly
dev.off()

### put the historical data and forecast data together, and save it
forecast.tmp <- forecast %>% dplyr::select(ds, yhat, trend) %>%
  mutate(ds = ymd(ds)) %>% filter(ds > last_history_date) 

final_dat <- bind_rows(hist_data %>% mutate(future = 0) %>% filter(ds <= last_history_date), 
                       forecast.tmp %>% rename(y = yhat) %>% mutate(future = 1))%>%
  mutate(future = as.factor(future)) %>% 
  select(future, ds, y) %>%
  left_join(forecast %>% select(ds, trend) %>% mutate(ds = lubridate::ymd(ds)), by = "ds") %>%
  mutate(MA91d = rollapplyr(y, list(-90:0), mean, fill=NA))
# write the complete final forecast data (with daily forecast, and 91d MA)
write.csv(final_dat, paste0(file_path, file_name, "upto_", last_history_date, "_withMA_bothgrowth.csv"))
############################################################################################################
  