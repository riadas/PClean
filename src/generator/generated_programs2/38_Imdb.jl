using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("actor_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("actor_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


omitted = []
if length(names(dirty_table)) != length(Any[Any[-1, "*"], Any[0, "aid"], Any[0, "gender"], Any[0, "name"], Any[0, "nationality"], Any[0, "birth city"], Any[0, "birth year"], Any[1, "id"], Any[1, "msid"], Any[1, "cid"], Any[2, "id"], Any[2, "msid"], Any[2, "aid"], Any[2, "role"], Any[3, "gid"], Any[3, "genre"], Any[4, "id"], Any[4, "msid"], Any[4, "gid"], Any[5, "id"], Any[5, "name"], Any[5, "country code"], Any[6, "did"], Any[6, "gender"], Any[6, "name"], Any[6, "nationality"], Any[6, "birth city"], Any[6, "birth year"], Any[7, "pid"], Any[7, "gender"], Any[7, "name"], Any[7, "nationality"], Any[7, "birth city"], Any[7, "birth year"], Any[8, "id"], Any[8, "msid"], Any[8, "did"], Any[9, "id"], Any[9, "keyword"], Any[10, "id"], Any[10, "msid"], Any[10, "pid"], Any[11, "mid"], Any[11, "title"], Any[11, "release year"], Any[11, "title aka"], Any[11, "budget"], Any[12, "id"], Any[12, "msid"], Any[12, "kid"], Any[13, "sid"], Any[13, "title"], Any[13, "release year"], Any[13, "num of seasons"], Any[13, "num of episodes"], Any[13, "title aka"], Any[13, "budget"], Any[14, "wid"], Any[14, "gender"], Any[14, "name"], Any[14, "nationality"], Any[14, "num of episodes"], Any[14, "birth city"], Any[14, "birth year"], Any[15, "id"], Any[15, "msid"], Any[15, "wid"]])
    for dirty_name in names(dirty_table)
        if !(lowercase(join(split(dirty_name, " "), "")) in map(tup -> lowercase(join(split(tup[2], "_"), "")), Any[Any[-1, "*"], Any[0, "aid"], Any[0, "gender"], Any[0, "name"], Any[0, "nationality"], Any[0, "birth city"], Any[0, "birth year"], Any[1, "id"], Any[1, "msid"], Any[1, "cid"], Any[2, "id"], Any[2, "msid"], Any[2, "aid"], Any[2, "role"], Any[3, "gid"], Any[3, "genre"], Any[4, "id"], Any[4, "msid"], Any[4, "gid"], Any[5, "id"], Any[5, "name"], Any[5, "country code"], Any[6, "did"], Any[6, "gender"], Any[6, "name"], Any[6, "nationality"], Any[6, "birth city"], Any[6, "birth year"], Any[7, "pid"], Any[7, "gender"], Any[7, "name"], Any[7, "nationality"], Any[7, "birth city"], Any[7, "birth year"], Any[8, "id"], Any[8, "msid"], Any[8, "did"], Any[9, "id"], Any[9, "keyword"], Any[10, "id"], Any[10, "msid"], Any[10, "pid"], Any[11, "mid"], Any[11, "title"], Any[11, "release year"], Any[11, "title aka"], Any[11, "budget"], Any[12, "id"], Any[12, "msid"], Any[12, "kid"], Any[13, "sid"], Any[13, "title"], Any[13, "release year"], Any[13, "num of seasons"], Any[13, "num of episodes"], Any[13, "title aka"], Any[13, "budget"], Any[14, "wid"], Any[14, "gender"], Any[14, "name"], Any[14, "nationality"], Any[14, "num of episodes"], Any[14, "birth city"], Any[14, "birth year"], Any[15, "id"], Any[15, "msid"], Any[15, "wid"]]))
            push!(omitted, dirty_name)
        end
    end
end
dirty_columns = filter(n -> !(n in omitted), names(dirty_table))

## construct possibilities
foreign_keys = ["msid", "aid", "msid", "gid", "did", "msid", "pid", "msid", "msid", "wid", "msid"]
column_names_without_foreign_keys = Any[Any[-1, "*"], Any[0, "gender"], Any[0, "name"], Any[0, "nationality"], Any[0, "birth city"], Any[0, "birth year"], Any[1, "id"], Any[1, "cid"], Any[2, "id"], Any[2, "role"], Any[3, "genre"], Any[4, "id"], Any[5, "id"], Any[5, "name"], Any[5, "country code"], Any[6, "gender"], Any[6, "name"], Any[6, "nationality"], Any[6, "birth city"], Any[6, "birth year"], Any[7, "gender"], Any[7, "name"], Any[7, "nationality"], Any[7, "birth city"], Any[7, "birth year"], Any[8, "id"], Any[9, "id"], Any[9, "keyword"], Any[10, "id"], Any[11, "mid"], Any[11, "title"], Any[11, "release year"], Any[11, "title aka"], Any[11, "budget"], Any[12, "id"], Any[12, "kid"], Any[13, "sid"], Any[13, "title"], Any[13, "release year"], Any[13, "num of seasons"], Any[13, "num of episodes"], Any[13, "title aka"], Any[13, "budget"], Any[14, "gender"], Any[14, "name"], Any[14, "nationality"], Any[14, "num of episodes"], Any[14, "birth city"], Any[14, "birth year"], Any[15, "id"]]
if length(omitted) == 0 
    column_renaming_dict = Dict(zip(dirty_columns, map(t -> t[2], column_names_without_foreign_keys)))
    column_renaming_dict_reverse = Dict(zip(map(t -> t[2], column_names_without_foreign_keys), dirty_columns))
else
    column_renaming_dict = Dict(zip(sort(dirty_columns), sort(map(t -> t[2], column_names_without_foreign_keys))))
    column_renaming_dict_reverse = Dict(zip(sort(map(t -> t[2], column_names_without_foreign_keys)), sort(dirty_columns)))    
end

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in dirty_columns
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model ImdbModel begin
    @class Actor begin
        aid ~ Unmodeled()
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Copyright begin
        id ~ Unmodeled()
        msid ~ ChooseUniformly(possibilities[:msid])
        cid ~ ChooseUniformly(possibilities[:cid])
    end

    @class Cast begin
        id ~ Unmodeled()
        copyright ~ Copyright
        actor ~ Actor
        role ~ ChooseUniformly(possibilities[:role])
    end

    @class Genre begin
        gid ~ Unmodeled()
        genre ~ ChooseUniformly(possibilities[:genre])
    end

    @class Classification begin
        id ~ Unmodeled()
        copyright ~ Copyright
        genre ~ Genre
    end

    @class Company begin
        id ~ Unmodeled()
        name ~ ChooseUniformly(possibilities[:name])
        country_code ~ ChooseUniformly(possibilities[:country_code])
    end

    @class Director begin
        did ~ Unmodeled()
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Producer begin
        pid ~ Unmodeled()
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Directed_By begin
        id ~ Unmodeled()
        copyright ~ Copyright
        director ~ Director
    end

    @class Keyword begin
        id ~ Unmodeled()
        keyword ~ ChooseUniformly(possibilities[:keyword])
    end

    @class Made_By begin
        id ~ Unmodeled()
        copyright ~ Copyright
        producer ~ Producer
    end

    @class Movie begin
        mid ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        release_year ~ ChooseUniformly(possibilities[:release_year])
        title_aka ~ ChooseUniformly(possibilities[:title_aka])
        budget ~ ChooseUniformly(possibilities[:budget])
    end

    @class Tags begin
        id ~ Unmodeled()
        copyright ~ Copyright
        kid ~ ChooseUniformly(possibilities[:kid])
    end

    @class Tv_Series begin
        sid ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        release_year ~ ChooseUniformly(possibilities[:release_year])
        num_of_seasons ~ ChooseUniformly(possibilities[:num_of_seasons])
        num_of_episodes ~ ChooseUniformly(possibilities[:num_of_episodes])
        title_aka ~ ChooseUniformly(possibilities[:title_aka])
        budget ~ ChooseUniformly(possibilities[:budget])
    end

    @class Writer begin
        wid ~ Unmodeled()
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        num_of_episodes ~ ChooseUniformly(possibilities[:num_of_episodes])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Written_By begin
        id ~ Unmodeled()
        copyright ~ Copyright
        writer ~ Writer
    end

    @class Obs begin
        cast ~ Cast
        classification ~ Classification
        company ~ Company
        directed_By ~ Directed_By
        keyword ~ Keyword
        made_By ~ Made_By
        movie ~ Movie
        tags ~ Tags
        tv_Series ~ Tv_Series
        written_By ~ Written_By
    end
end

query = @query ImdbModel.Obs [
    actor_aid cast.actor.aid
    actor_gender cast.actor.gender
    actor_name cast.actor.name
    actor_nationality cast.actor.nationality
    actor_birth_city cast.actor.birth_city
    actor_birth_year cast.actor.birth_year
    copyright_id cast.copyright.id
    copyright_msid cast.copyright.msid
    copyright_cid cast.copyright.cid
    cast_id cast.id
    cast_role cast.role
    genre_gid classification.genre.gid
    genre classification.genre.genre
    classification_id classification.id
    company_id company.id
    company_name company.name
    company_country_code company.country_code
    director_did directed_By.director.did
    director_gender directed_By.director.gender
    director_name directed_By.director.name
    director_nationality directed_By.director.nationality
    director_birth_city directed_By.director.birth_city
    director_birth_year directed_By.director.birth_year
    producer_pid made_By.producer.pid
    producer_gender made_By.producer.gender
    producer_name made_By.producer.name
    producer_nationality made_By.producer.nationality
    producer_birth_city made_By.producer.birth_city
    producer_birth_year made_By.producer.birth_year
    directed_by_id directed_By.id
    keyword_id keyword.id
    keyword keyword.keyword
    made_by_id made_By.id
    movie_mid movie.mid
    movie_title movie.title
    movie_release_year movie.release_year
    movie_title_aka movie.title_aka
    movie_budget movie.budget
    tags_id tags.id
    tags_kid tags.kid
    tv_series_sid tv_Series.sid
    tv_series_title tv_Series.title
    tv_series_release_year tv_Series.release_year
    tv_series_num_of_seasons tv_Series.num_of_seasons
    tv_series_num_of_episodes tv_Series.num_of_episodes
    tv_series_title_aka tv_Series.title_aka
    tv_series_budget tv_Series.budget
    writer_wid written_By.writer.wid
    writer_gender written_By.writer.gender
    writer_name written_By.writer.name
    writer_nationality written_By.writer.nationality
    writer_num_of_episodes written_By.writer.num_of_episodes
    writer_birth_city written_By.writer.birth_city
    writer_birth_year written_By.writer.birth_year
    written_by_id written_By.id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
