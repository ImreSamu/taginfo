

# julia -- ./taginfo/sources/db/normalized_names.jl ./data/taginfo-db.db ./download/normalized_names.xlsx
# Julia v0.6


using SQLite, DataFrames, XLSX


println("db   = ",ARGS[1])
println("xlsx = ",ARGS[2])


#db = SQLite.DB("./data/taginfo-db.db")
db = SQLite.DB( ARGS[1] )

@register db function julia_normalize_string(s)
            replace( replace(
                normalize_string(s,
					  decompose=true,
					  compat=true,
					  casefold=true,
					  stripmark=true,
					  stripignore=true,
                      stripcc=true)
                , " ","")
                , "-","")
       end

SQLite.drop!(db, "temp_normalized_names", ifexists=true)
SQLite.query(db, """
    create TEMPORARY table temp_normalized_names as
    select * from (
        select
          key2                           as keyname
        , julia_normalize_string(value2) as normalized_keyname_value
        , value2                         as keyname_value
        , key1                           as k
        , value1                         as v
        , count_all
        from tag_combinations
        where
            key1  in ('amenity', 'shop', 'leisure', 'man_made', 'tourism'
                     ,'craft','sport','emergency','historic','healthcare', 'military'
                     ,'university', 'club', 'golf' , 'playground'
                     ,'landuse', 'natural', 'waterway', 'waterway:sign', 'seamark:type'
                     ,'power', 'pipeline'
                     ,'aeroway', 'aerialway'
                     ,'boundary', 'place'
                     ,'highway', 'railway'
                     ,'public_transport', 'junction', 'barrier', 'entrance', 'route'
                     ,'building','building:part'
                     )
        and (key2 in ( 'name'
                     ,'int_name','loc_name','nat_name','official_name'
                     ,'old_name','reg_name','alt_name','short_name'
                     ,'species','genus','taxon'
                     ,'addr:street','addr:city','addr:country','addr:state'
                     ,'addr:district','addr:region','addr:hamlet','addr:place'
                     ,'parish','diocese','deanery'
                     ,'architect'
                     )
            or key2 like '%name:%'
            or key2 like 'species:%'
            or key2 like 'genus:%'
            or key2 like 'taxon:%'            
            or key2 like 'addr:street:%'
            or key2 like 'addr:city:%'
            or key2 like 'addr:hamlet:%'
            or key2 like 'addr:place:%'
            )
        and value1 !=''
        and value2 !=''
      union all
        select
          key1                           as keyname
        , julia_normalize_string(value1) as normalized_keyname_value
        , value1                         as keyname_value
        , key2                           as k
        , value2                         as v
        , count_all
        from tag_combinations
        where
             key2  in ('amenity', 'shop', 'leisure', 'man_made', 'tourism'
             ,'craft','sport','emergency','historic','healthcare', 'military'
             ,'university', 'club', 'golf' , 'playground'
             ,'landuse', 'natural', 'waterway', 'waterway:sign', 'seamark:type'
             ,'power', 'pipeline'
             ,'aeroway', 'aerialway'
             ,'boundary', 'place'
             ,'highway', 'railway'
             ,'public_transport', 'junction', 'barrier', 'entrance', 'route'
             ,'building','building:part'
             )
        and (key1 in ( 'name'
                      ,'int_name','loc_name','nat_name','official_name'
                      ,'old_name','reg_name','alt_name','short_name'
                      ,'species','genus','taxon'
                      ,'addr:street','addr:city','addr:country','addr:state'
                      ,'addr:district','addr:region','addr:hamlet','addr:place'
                      ,'parish','diocese','deanery'
                      ,'architect'                      
                      )
             or key1 like '%name:%'
             or key1 like 'species:%'
             or key1 like 'genus:%'
             or key2 like 'taxon:%'             
             or key1 like 'addr:street:%'
             or key1 like 'addr:city:%'
             or key1 like 'addr:hamlet:%'
             or key1 like 'addr:place:%'
             )
        and value1 !=''
        and value2 !=''
        ) as t
    order by 1,2
    ;
""")


#result = SQLite.query(db, """
#    select * from temp_normalized_names limit 30;
#    ;
#    """)


SQLite.drop!(db, "temp_normalized_names_2", ifexists=true)
SQLite.query(db, """
    create TEMPORARY table temp_normalized_names_2 as
    with d as
    (   SELECT keyname,normalized_keyname_value, k, v
        FROM temp_normalized_names
        GROUP BY keyname, normalized_keyname_value , k, v
        HAVING count(*) > 1
        ORDER BY keyname, normalized_keyname_value , k, v
    )
    select l.k
          ,l.v
          ,l.keyname
          ,l.normalized_keyname_value
          ,l.keyname_value
          ,l.count_all
    from temp_normalized_names as l
    join d
    where     l.keyname=d.keyname
          and l.normalized_keyname_value=d.normalized_keyname_value
          and l.k=d.k
          and l.v=d.v
    order by 1,2,3,4
;
""")

normalized_names = SQLite.query(db, """
    select * from temp_normalized_names_2;
    """)


# workaround: https://github.com/JuliaDatabases/SQLite.jl/issues/147
SQLite.drop!(db, "normalized_names", ifexists=true)
sink_normalized_names = SQLite.Sink(db, "normalized_names", Data.schema(normalized_names))
SQLite.load(sink_normalized_names, normalized_names)



SQLite.query(db, "CREATE INDEX normalized_names_k_idx                        ON normalized_names (k);")
SQLite.query(db, "CREATE INDEX normalized_names_v_idx                        ON normalized_names (v);")
SQLite.query(db, "CREATE INDEX normalized_names_keyname_idx                  ON normalized_names (keyname);")
SQLite.query(db, "CREATE INDEX normalized_names_normalized_keyname_value_idx ON normalized_names (normalized_keyname_value);")
SQLite.query(db, "CREATE INDEX normalized_names_keyname_value_idx            ON normalized_names (keyname_value);")
SQLite.query(db, "CREATE INDEX normalized_names_idx                          ON normalized_names (k,v,keyname,normalized_keyname_value);")

SQLite.query(db, "ANALYZE normalized_names;")



XLSX.writetable( ARGS[2], DataFrames.columns(normalized_names), DataFrames.names(normalized_names), rewrite=false,sheetname="normalized_names")
