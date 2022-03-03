drop table if exists BTSOriginAndDestinationSurvey;
create table if not exists BTSOriginAndDestinationSurvey (
    id bigint not null primary key auto_increment,
    timestamp timestamp not null default current_timestamp on update current_timestamp,
    captured datetime not null default current_timestamp,
    ItinID bigint,
    MktID bigint,unique key ItinID_key (ItinID,MktID),
    MktCoupons int,
    Year int,
    Quarter int,
    period_end date as (last_day(cast(concat(year,'-',Quarter*3,-'01') as date))) stored,key period_end_key (period_end),
    OriginAirportID int,
    OriginAirportSeqID int,
    OriginCityMarketID int,
    Origin char(3),key Origin_key (Origin),#constraint BTSOADS_Origin_fkey foreign key (Origin) references Airports (AirportCode),
    OriginCountry char(2),key OriginCountry_key (OriginCountry),
    OriginStateFips int,
    OriginState char(2),
    OriginStateName varchar(64),
    OriginWac int,
    DestAirportID int,
    DestAirportSeqID int,
    DestCityMarketID int,
    Dest char(3),key Dest_key (Dest),#constraint BTSOADS_Dest_fkey foreign key (Dest) references Airports (AirportCode),
    DestCountry char(2),key DestCountry_key (DestCountry),
    DestStateFips int,
    DestState char(2),
    DestStateName varchar(64),
    DestWac int,
    AirportGroup varchar(64),
    WacGroup varchar(64),
    TkCarrierChange int,
    TkCarrierGroup varchar(64),
    OpCarrierChange int,
    OpCarrierGroup varchar(64),
    RPCarrier char(2),
    TkCarrier char(2),
    OpCarrier char(2),
    BulkFare bit,
    Passengers int,
    MktFare decimal(8,2),
    MktDistance double,
    MktDistanceGroup int,
    MktMilesFlown double,
    NonStopMiles double,
    ItinGeoType int,
    MktGeoType int
);

load data infile
    '/tmp/Origin_and_Destination_Survey_DB1BMarket_2018_1.csv'
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
    TkCarrierChange=@TkCarrierChange+0,
    OpCarrierChange=@OpCarrierChange+0,
    BulkFare=@BulkFare+0;