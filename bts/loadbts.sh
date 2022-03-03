#!/bin/bash

mysql --user=$USER --password=$MYSQLPASSWORD Analysis <<EOD && echo `date`: truncated table || exit $?
truncate table BTSOriginAndDestinationSurvey
EOD

ls /tmp/Origin_and_Destination_Survey_*.csv | while read file
do
    mysql --user=$USER --password=$MYSQLPASSWORD Analysis <<EOD && echo `date`: $file || exit $?
load data infile
    '$file'
replace into table
    BTSOriginAndDestinationSurvey
columns terminated by ','
optionally enclosed by '"'
ignore 1 lines
(
    ItinID,
    MktID,
    MktCoupons,
    Year,
    Quarter,
    OriginAirportID,
    OriginAirportSeqID,
    OriginCityMarketID,
    Origin,
    OriginCountry,
    OriginStateFips,
    OriginState,
    OriginStateName,
    OriginWac,
    DestAirportID,
    DestAirportSeqID,
    DestCityMarketID,
    Dest,
    DestCountry,
    DestStateFips,
    DestState,
    DestStateName,
    DestWac,
    AirportGroup,
    WacGroup,
    @TkCarrierChange,
    TkCarrierGroup,
    @OpCarrierChange,
    OpCarrierGroup,
    RPCarrier,
    TkCarrier,
    OpCarrier,
    @BulkFare,
    Passengers,
    MktFare,
    MktDistance,
    MktDistanceGroup,
    MktMilesFlown,
    NonStopMiles,
    ItinGeoType,
    MktGeoType
)
set
    TkCarrierChange=case @TkCarrierChange when '1' then 1 when '0' then 0 end,
    OpCarrierChange=case @OpCarrierChange when '1' then 1 when '0' then 0 end,
    BulkFare=case @BulkFare when '1' then 1 when '0' then 0 end;
EOD
done

