#' Get clam data in 2017+ strata
#'
#' New clam strata were defined starting in 2017 and no tows occur outside of these strata after 2016. 
#' Recent clam assessments are now using
#' this set of strata. The data sampled from previous years surveys are now required on this
#' new footprint. The data are pulled from the survey database and stations/tows that fall within
#' the boundaries of the new strata are selected (using strata shapefile). All other tows are
#' discarded
#'
#' A connection to SVDBS is needed. survdat is used to pull the clam data

# connect to db however you normally do.
# if you can install dbutils then this is the best option to connect
#channel <- dbutils::connect_to_database("sole","user")
#conn <- dbConnect(drv,username=user,password=passwd, dbname=connect.string)

library(data.table)

source(("C:\\Users\\laurel.smith\\Documents\\EDAB\\ConditionGAM\\R\\ConnectOracle.R"))


#read in stratum/Region pairs
regions <- read.csv(here::here("data-raw/gis/SCStrataData.csv"),header = T) |>
  dplyr::as_tibble()

# old shellfish polygons from earlier surveys (sf object)
oldpoly <- NEFSCspatial::Shellfish_Strata

# read in new clam shapefile (North/South) and joins to stratum/Region (sf object)
newStrata <-sf::st_read(dsn=here::here("data-raw/gis/SCstrata.shp")) |>
  dplyr::left_join(regions, by = c("name"="Group.1")) |>
  dplyr::select(name,col,Region,geometry) |>
  sf::st_set_crs(sf::st_crs(oldpoly))

# pulls survdat data and remove missing lat and longs
surv <- survdat::get_survdat_clam_data(channel, assignRegionWeights = F)
surv <- surv$data |>
  dplyr::filter(!(is.na(LAT) | is.na(LON) ))

# converts survdat data to sf object so we can clip the data to strata
survdat_points <- sf::st_as_sf(surv,coords=c("LON","LAT"),crs=sf::st_crs(oldpoly))
survdat_points <- survdat_points %>% mutate(STRATUM = as.numeric(STRATUM))

#*********
#Old school SQL data retrieval:
#Albatross and Delaware data 1982-2008, all seasons:
qry <- c(
  "select b.cruise6,b.stratum,b.tow,b.station,
  s.est_year year,season, est_month month,est_day day,
  substr(est_time,1,2)||substr(est_time,4,2) time,
  round(substr(beglat,1,2) + (substr(beglat,3,7)/60),6) lat,  
  round(substr(endlat,1,2) + (substr(endlat,3,7)/60),6) endlat,
  round(((substr(beglon,1,2) + (substr(beglon,3,7)/60)) * -1), 6) lon,
  round(((substr(endlon,1,2) + (substr(endlon,3,7)/60)) * -1), 6) endlon,
  towdur, setdepth, enddepth, mindepth, maxdepth, avgdepth,
  surftemp, bottemp, surfsalin, botsalin, 
  b.svspp, b.catchsex, expcatchwt biomass, expcatchnum abundance, length, expnumlen numlen, s.svvessel, dopdistb
  
  from svdbs.UNION_FSCS_SVLEN b, svdbs.UNION_FSCS_SVCAT p, svdbs.UNION_FSCS_SVSTA s, svdbs.mstr_cruise c
  where
  (b.cruise6=s.cruise6) and
  (c.cruise6=b.cruise6) and
  (p.cruise6=c.cruise6) and
  (p.stratum=b.stratum) and
  (b.stratum=s.stratum) and
  (p.station=b.station) and
  (b.station=s.station) and
  (p.tow=b.tow) and
  (b.tow=s.tow) and
  (c.svvessel=s.svvessel) and
  (p.svspp=b.svspp) and
  b.svspp in ('403', '409') and
  (p.catchsex=b.catchsex) and 
  year >=1982 and 
  purpose_code = 50 and
  (SHG <= 136)
  order by year, cruise6, station, svspp, catchsex, length "
)

#removed species selection from SQL:
# b.svspp in ('013','015','023','026','028','032','072','073','074','075','076','077','078','102','103','104','105','106','107','108','121','131','135','141','143','145','155','164','193','197') and
# not included for clams data: and p.cruise6 <= 200900)

survey <- DBI::dbGetQuery(channel, qry)
#survey1 <- as.data.table(survey)
survey1 <- survey %>% mutate(STRATUM = as.numeric(STRATUM)) %>% 
  select(CRUISE6, STATION, STRATUM, SVSPP, CATCHSEX, LENGTH, TOWDUR, DOPDISTB, SETDEPTH, ENDDEPTH, MINDEPTH, MAXDEPTH, AVGDEPTH)

#Merge survdat_clam_data with swept-area data:
#merge with survdat
#data.table::setkey(survdat_points, CRUISE6, STATION, STRATUM, SVSPP, CATCHSEX, LENGTH)
#data.table::setkey(survey1, CRUISE6, STATION, STRATUM, SVSPP, CATCHSEX, LENGTH)
survdat_clams <- left_join(survdat_points, survey1, by = c('CRUISE6', 'STATION', 'STRATUM', 'SVSPP', 'CATCHSEX', 'LENGTH'))


#if meat weight isn't needed, can use all data not clipped to new strata:
saveRDS(survdat_clams, file = here::here('output','Clamdata_OldNewStrata_10-24-2024.RDS'))

#************************


#Survdat code (survdat::get_survdat_clam_data) to add other fields to data selection (to get swept-area):
  function (channel, shg.check = T, clam.only = T, tidy = F, assignRegionWeights = T) 
  {
    call <- capture_function_call()
    cruise.qry <- "select unique year, cruise6, svvessel\n                 
    from svdbs.mstr_cruise\n                 
    where purpose_code = 50\n                 
    and year >= 1982\n                 
    order by year, cruise6"
    cruise <- data.table::as.data.table(DBI::dbGetQuery(channel, 
                                                        cruise.qry))
    cruise <- na.omit(cruise)
    data.table::setkey(cruise, CRUISE6, SVVESSEL)
    cruise6 <- survdat:::sqltext(cruise$CRUISE6)
    if (shg.check == T) {
      station.qry <- paste("select unique cruise6, svvessel, station, stratum, decdeg_beglat as lat, decdeg_beglon as lon,                  
                           avgdepth as depth, surftemp, surfsalin, bottemp, botsalin                   
                           from svdbs.Union_fscs_svsta                   
                           where cruise6 in (", 
                           cruise6, ")                   
                           and SHG <= 136                   
                           order by cruise6, station", 
                           sep = "")
    }
    else {
      station.qry <- paste("select unique cruise6, svvessel, station, stratum, decdeg_beglat as lat, decdeg_beglon as lon,                   avgdepth as depth, surftemp, surfsalin, bottemp, botsalin\n                   from svdbs.UNION_FSCS_SVSTA\n                   where cruise6 in (", 
                           cruise6, ")                   
                           order by cruise6, station", 
                           sep = "")
    }
    station <- data.table::as.data.table(DBI::dbGetQuery(channel, 
                                                         station.qry))
    data.table::setkey(station, CRUISE6, SVVESSEL)
    clamdat <- base::merge(cruise, station)
    if (clam.only == T) {
      catch.qry <- paste("select cruise6, station, stratum, svspp, catchsex, expcatchnum as abundance, expcatchwt as biomass                 
                         from svdbs.UNION_FSCS_SVCAT                 
                         where cruise6 in (", 
                         cruise6, ")                 
                         and svspp in ('403', '409')                 
                         order by cruise6, station, svspp", 
                         sep = "")
    }
    else {
      catch.qry <- paste("select cruise6, station, stratum, svspp, catchsex, expcatchnum as abundance, expcatchwt as biomass
                         from svdbs.UNION_FSCS_SVCAT
                         where cruise6 in (", 
                         cruise6, ")
                         order by cruise6, station, svspp", 
                         sep = "")
    }
    catch <- data.table::as.data.table(DBI::dbGetQuery(channel, 
                                                       catch.qry))
    data.table::setkey(catch, CRUISE6, STATION, STRATUM)
    data.table::setkey(clamdat, CRUISE6, STATION, STRATUM)
    clamdat <- base::merge(clamdat, catch, all.x = T)
    if (clam.only == T) {
      length.qry <- paste("select cruise6, station, stratum, svspp, catchsex, length, expnumlen as numlen
                          from svdbs.UNION_FSCS_SVLEN
                          where cruise6 in (", 
                          cruise6, ")
                          and svspp in ('403', '409')
                          order by cruise6, station, svspp, length", 
                          sep = "")
    }
    else {
      length.qry <- paste("select cruise6, station, stratum, svspp, catchsex, length, expnumlen as numlen
                          from svdbs.UNION_FSCS_SVLEN
                          where cruise6 in (", 
                          cruise6, ")
                          order by cruise6, station, svspp, length", 
                          sep = "")
    }
    len <- data.table::as.data.table(DBI::dbGetQuery(channel, 
                                                     length.qry))
    data.table::setkey(len, CRUISE6, STATION, STRATUM, SVSPP, 
                       CATCHSEX)
    data.table::setkey(clamdat, CRUISE6, STATION, STRATUM, SVSPP, 
                       CATCHSEX)
    clamdat <- base::merge(clamdat, len, all.x = T)
    clamdat[, `:=`(STRATUM, as.numeric(STRATUM))]
    if (assignRegionWeights) {
      regions <- c("SVA", "DMV", "SNJ", "NNJ", "LI", "SNE", 
                   "GB")
      SVA <- c(6010:6080, 6800, 6810)
      DMV <- c(6090:6160, 6820:6860)
      SNJ <- c(6170:6200, 6870)
      NNJ <- c(6210:6280, 6880:6900)
      LI <- c(6290:6360, 6910:6930)
      SNE <- c(6370:6520, 6940:6960)
      GB <- c(6530:6740)
      clamdat[, `:=`(clam.region, factor(NA, levels = regions))]
      for (i in 1:length(regions)) clamdat[STRATUM %in% get(regions[i]), 
                                           `:=`(clam.region, regions[i])]
      coeff <- data.table::data.table(clam.region = c("SVA", 
                                                      "DMV", "SNJ", "NNJ", "LI", "SNE", "GB"), oq.a = c(-9.04231, 
                                                                                                        -9.04231, -9.84718, -9.84718, -9.23365, -9.12428, 
                                                                                                        -8.96907), oq.b = c(2.787987, 2.787987, 2.94954, 
                                                                                                                            2.94954, 2.822474, 2.774989, 2.767282), sc.a = c(-7.0583, 
                                                                                                                                                                             -9.48913, -9.3121, -9.3121, -7.9837, -7.9837, -8.27443), 
                                      sc.b = c(2.3033, 2.860176, 2.863716, 2.863716, 2.5802, 
                                               2.5802, 2.654215))
      coeff[, `:=`(clam.region, as.factor(clam.region))]
      clamdat <- base::merge(clamdat, coeff, by = "clam.region")
      clamdat[SVSPP == 403, `:=`(meatwt, (exp(sc.a) * (LENGTH * 
                                                         10)^sc.b)/1000)]
      clamdat[SVSPP == 409, `:=`(meatwt, (exp(oq.a) * (LENGTH * 
                                                         10)^oq.b)/1000)]
      clamdat[, `:=`(expmw, meatwt * NUMLEN)]
      clamdat[, `:=`(stamw, sum(expmw)), by = c("CRUISE6", 
                                                "STRATUM", "STATION", "SVSPP")]
      clamdat[, `:=`(c("oq.a", "oq.b", "sc.a", "sc.b", "meatwt", 
                       "expmw"), NULL)]
      data.table::setnames(clamdat, "stamw", "BIOMASS.MW")
    }
    if (tidy) {
      clamdat <- tibble::as_tibble(clamdat)
    }
    sql <- list(cruise = cruise.qry, station = station.qry, catch = catch.qry, 
                length = length.qry)
    return(list(data = clamdat, sql = sql, pullDate = date(), 
                functionCall = call))
  }
<bytecode: 0x000001c6dfacf070>
<environment: namespace:survdat>
  
  
#*******************************




### plot to see how the data relate to the strata
p1 <- ggplot2::ggplot() +
  ggplot2::geom_sf(data = newStrata) +
  ggplot2::geom_sf(data = survdat_points, size=.7, color=ggplot2::alpha("grey",0.2)) +
  ggplot2::theme_minimal() +
  ggplot2::ggtitle(paste0("All survey data ", paste0(range(survdat_points$YEAR),collapse = "-")))
print(p1)

## plot points with old survey polygons as a comparison
p2 <- ggplot2::ggplot() +
  ggplot2::geom_sf(data = oldpoly) +
  ggplot2::geom_sf(data = survdat_points,size=.7,color=ggplot2::alpha("grey",0.2)) +
  ggplot2::theme_minimal() +
  ggplot2::ggtitle(paste0("All survey data ", paste0(range(survdat_points$YEAR),collapse = "-")))
print(p2)

## select the lat and longs in the survdat data that fall withing the new
# survey strata. point in polygon operation (assign points to polygons)
joinPointsToPolygon <- sf::st_join(survdat_points,newStrata) |>
  dplyr::filter(!is.na(name))

# plot the new strata with the data that falls within these strata
p3 <- ggplot2::ggplot() +
  ggplot2::geom_sf(data = newStrata) +
  ggplot2::geom_sf(data = joinPointsToPolygon,
                   size=.7,
                   color=ggplot2::alpha("grey",0.2)) +
  ggplot2::theme_minimal() +
  ggplot2::ggtitle(paste0("All survey data clipped to strata definitions of 2016 (", paste0(range(survdat_points$YEAR),collapse = "-"),")"))

plot(p3)

### The clipped data can now be used to assign length to meat conversions

clippedData <- joinPointsToPolygon

# Calculating meat weights based on clam length (separately for nothern v. southern stocks)
# length-meat weight coefficients from Dan Hennen on Jan. 2nd, 2023:
# b = 2.73325 (slope for both stocks)
# a = 9.44477e-05 (intercept south - this value converts length in cm to weight in kg)
# a = 0.0001055 (intercept north) 
# w = aL^b

ClamCoeff <- clippedData %>% dplyr::mutate(b = 2.73325) %>% 
  dplyr::mutate(a = if_else(Region == 'NORTH', 0.0001055,
                if_else(Region == 'SOUTH', 9.44477e-05, 0)))

ClamMeatWt <- ClamCoeff %>% dplyr::mutate(MeatWtKg = a*LENGTH^b)

saveRDS(ClamMeatWt, file = here::here('output','ClamdatMeatWt.RDS'))
