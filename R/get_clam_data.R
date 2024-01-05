#' Get clam data in 2017+ strata
#'
#' New clam strata were defined from 2017. Recent clam assessments are now using
#' this set of strata. The data sampled from previous years surveys are now required on this
#' new footprint. The data are pulled from the survey database and stations/tows that fall within
#' the boundaries of the new strata are selected (using strata shapefile). All other tows are
#' discarded
#'
#' A connection to SVDBS is needed. survdat is used to pull the clam data

# connect to db however you normally do.
# if you can install dbutils then this is the best option to connect
channel <- dbutils::connect_to_database("sole","user")


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
surv <- survdat::get_survdat_clam_data(channel,assignRegionWeights = F)
surv <- surv$data |>
  dplyr::filter(!(is.na(LAT) | is.na(LON) ))

# converts survdat data to sf object so we can clip the data to strata
survdat_points <- sf::st_as_sf(surv,coords=c("LON","LAT"),crs=sf::st_crs(oldpoly))

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



