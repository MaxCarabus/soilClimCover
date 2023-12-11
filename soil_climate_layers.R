# 2023-12-08
# 4я стадия - генерация слоев почвенного климата и оценки их качества

setwd('/media/carabus/CARABUS20162/projects/EW_SDM/predictors/meteo/')

library(DBI)
library(sf)
library(geodata)
library(ggplot2)

connection <- dbConnect(RSQLite::SQLite(), "databases/soil_climate.sqlite")

# выбираем среднемесячные температуры
dbListTables(connection)

dbListFields(connection,'soil_temperature_month')

mType = 'p' # вытяжные термометры

# сводные данные по вытяжным термометрам 
# сводная таблица по грубинам
select = paste0('SELECT depth, count(t), min(year), max(year), ',
                'count(DISTINCT year), count(DISTINCT station_code) ',
                'FROM soil_temperature_month ',
                'WHERE measure_type = \'',mType,'\' ',
                'GROUP BY depth;')
result = dbGetQuery(connection, select)

# выбрали слои 20 (264 метеостанции), 40 (262) и 80 (264) см


# сводная таблица по годам
select = paste0('SELECT year, count(DISTINCT depth), count(t), min(depth), ',
                ' max(depth), count(DISTINCT station_code) ',
                'FROM soil_temperature_month ',
                'WHERE measure_type = \'',mType,'\' ',
                'GROUP BY year;')
result = dbGetQuery(connection, select)

# три глубины: 20, 40, 80 см
select = paste0('SELECT year, count(DISTINCT depth), count(t), min(depth), ',
                ' max(depth), count(DISTINCT station_code) ',
                'FROM soil_temperature_month ',
                'WHERE measure_type = \'',mType,'\' AND depth IN (20,40,80) ',
                'GROUP BY year;')
result = dbGetQuery(connection, select)


aoi = st_read('../../vector/Area_of_Interest/Russia_European_albers.shp')
plot(st_geometry(aoi))

station_list = read.csv('data/stations_vniigmi-mcd.csv')
nrow(station_list) # 1127 записей

country = gadm('RU', path = '../../../../gis/cache/gadm/', level = 0)
ruBorder = st_as_sf(country)
plot(st_geometry(ruBorder), xlim = c(25,180),
     ylim = c(min(station_list$latitude), max(station_list$longitude)))


plot(st_geometry(ruBorder), xlim = c(25,180),
     ylim = c(min(station_list$latitude), max(station_list$latitude)))


stations = st_as_sf(station_list, coords = c("longitude", "latitude"))
plot(st_geometry(stations), pch = 16, col = 'blue', cex = 0.6, add = T)
class(stations)

plot(st_geometry(aoi), add = T)

st_crs(aoi)

aoi_dd = st_transform(aoi, st_crs('EPSG:4326'))
plot(aoi_dd, add = T, lwd = 1.5, border = 'green')


aoiBuf128 = st_buffer(aoi,128000)
aoiBuf256 = st_buffer(aoi,256000)
plot(st_geometry(aoi))
plot(st_geometry(aoiBuf128), add = T, col = 'orange')
plot(st_geometry(aoiBuf256), add = T, col = 'yellow')

aoiBuf128_dd = st_transform(aoiBuf128, st_crs('EPSG:4326'))
aoiBuf256_dd = st_transform(aoiBuf256, st_crs('EPSG:4326'))

nrow(stations)
st_filter(stations, aoi_dd)

st_crs(aoi_dd)
st_crs(stations)

st_crs(stations) = st_crs('EPSG:4326')
st_crs(stations) = st_crs(aoi_dd)

ggplot() + 
  geom_sf(data = aoi_dd) +
  geom_sf(data = stations)

stAoi = st_filter(stations, aoi_dd) # 376 метеостанций в зоне интереса  
stAoi128 = st_filter(stations, aoiBuf128_dd) # 449 метеостанций с буффером 128 км
stAoi256 = st_filter(stations, aoiBuf256_dd) # 489 метеостанций с буффером 256 км

# составляем список метеостанций
select = paste0('SELECT station_code, count(t), min(year), max(year) ',
                'FROM soil_temperature_month ',
                'WHERE measure_type = \'',mType,'\' AND depth IN (20,40,80) ',
                'GROUP BY station_code;')
stDepth248List = dbGetQuery(connection, select)

                
select = paste0('SELECT station_code code, round(avg(t),3) mean_t ',
                'FROM soil_temperature_month ',
                'WHERE measure_type = \'',mType,'\' AND depth = 20 ',
                'GROUP BY station_code;')
depth20 = dbGetQuery(connection, select)
nrow(depth20) # 264

select = paste0('SELECT station_code code, round(avg(t),3) mean_t ',
                'FROM soil_temperature_month ',
                'WHERE measure_type = \'',mType,'\' AND depth = 40 ',
                'GROUP BY station_code;')
depth40 = dbGetQuery(connection, select)
nrow(depth40) # 262

select = paste0('SELECT station_code code, round(avg(t),3) mena_t ',
                'FROM soil_temperature_month ',
                'WHERE measure_type = \'',mType,'\' AND depth = 80 ',
                'GROUP BY station_code;')
depth80 = dbGetQuery(connection, select)
nrow(depth80) # 262


# все метеостанции с даннми по нужным глубинам
stDepth248List$station_code

# все метеостанции в зоне интереса с буфером 256 км
stAoi256$code

# 98 метеостанций
selectList = intersect(stDepth248List$station_code, stAoi256$code)

soilStations = stations[stations$code %in% selectList, ]
plot(st_geometry(soilStations), pch = 16, col = 'blue', cex = 2)
plot(st_geometry(aoiBuf256_dd), add = T)
#plot(st_geometry(stations), col = 'red', pch = 16, cex = 0.7, add = T)
plot(st_geometry(aoi_dd), add = T)



depth20aoi = merge(soilStations, depth20, by = 'code') 

depth20aoi$mean_t

plot(st_geometry(depth20aoi))

ggplot() +
  geom_sf(data = depth20aoi, aes(colour = mean_t))

library(raster)
blankRaster = raster(extent(aoiBuf256_dd), resolution = 0.1)

install.packages('fields')
library(fields)

soil20 = rasterize(depth20aoi, blankRaster, 'mean_t')
xy = data.frame(xyFromCell(soil20, 1: ncell(soil20)))
v <- getValues(soil20)
unique(v)
tps <- Tps(xy, v)
soil20raster = interpolate(soil20, tps)
plot(soil20raster)
soil20rasterm = crop(mask(soil20raster, aoi_dd),extent(aoi_dd))

plot(soil20rasterm)

colfunc  <- colorRampPalette(c('blue','cyan','green','yellow'))
plot(soil20rasterm, col = colfunc(32), main = 'Mean temperature at 20 cm depth')
plot(st_geometry(soilStations), pch = 16, col = 'blue', add = T)

extent(aoi_dd)

