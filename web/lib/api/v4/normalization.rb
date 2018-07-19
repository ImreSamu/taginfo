# web/lib/api/v4/wiki.rb
class Taginfo < Sinatra::Base

    api(4, 'normalization/names', {
        :description => 'List Names dups....',
        :parameters => {
            :query => 'Only show results where the (keyname_value) matches this query (substring match, optional).'
        },
        :paging => :optional,
        :result => paging_results([
            [:k                         , :STRING, 'k'],
            [:v                         , :STRING, 'v'],
            [:keyname                   , :STRING, 'keyname'],
            [:normalized_keyname_value  , :STRING, 'normalized_keyname_value'],
            [:keyname_value             , :STRING, 'keyname_value'],
            [:count_all                 , :INT,    'Number of keyname values']
        ]),
        :sort => %w( k v keyname normalized_keyname_value ),
        :example => { :sortname => 'keyname', :sortorder => 'desc' },
        :ui => '/reports/normalizednames'
    }) do

        total = @db.count('normalized_names').
            condition_if("keyname_value LIKE ? ESCAPE '@'", like_contains(params[:query])).
            get_first_i
#        total = @db.select("SELECT count(*) FROM normalized_names").get_first_value().to_i

        res = @db.select('SELECT * FROM normalized_names').
            condition_if("keyname_value LIKE ? ESCAPE '@'", like_contains(params[:query])).
            order_by(@ap.sortname, @ap.sortorder) { |o|
                o.k
                o.v
                o.keyname
                o.normalized_keyname_value
                o.keyname_value
                o.count_all
            }.
            paging(@ap).
            execute()

        return generate_json_result(total,
            res.map{ |row| {
                :k                       => row['k'],
                :v                       => row['v'],
                :keyname                 => row['keyname'],
                :normalized_keyname_value=> row['normalized_keyname_value'],
                :keyname_value           => row['keyname_value'],
                :count_all               => row['count_all'].to_i
            } }
        )
    end




    api(4, 'normalization/problematic_tags', {
        :description => 'List problematic tags....',
        :parameters => {
            :query => 'Only show results where the (key) matches this query (substring match, optional).'
        },
        :paging => :optional,
        :result => paging_results([
            [:key                       , :STRING, 'key'],
            [:value                     , :STRING, 'value'],
            [:count_all                 , :INT,    'Number of values']
        ]),
        :sort => %w( key value ),
        :example => { :sortname => 'key', :sortorder => 'asc' },
        :ui => '/reports/problematic_tags'
    }) do

        total = @db.count('problematic_tags').
            condition_if("key LIKE ? ESCAPE '@'", like_contains(params[:query])).
            get_first_i

        res = @db.select('SELECT * FROM problematic_tags').
            condition_if("key LIKE ? ESCAPE '@'", like_contains(params[:query])).
            order_by(@ap.sortname, @ap.sortorder) { |o|
                o.key
                o.value
                o.count_all
            }.
            paging(@ap).
            execute()

        return generate_json_result(total,
            res.map{ |row| {
                :key        => row['key'],
                :value      => row['value'],
                :count_all  => row['count_all'].to_i
            } }
        )
    end

end
