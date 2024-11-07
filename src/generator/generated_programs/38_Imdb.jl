using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("actor_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("actor_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "aid"], Any[0, "gender"], Any[0, "name"], Any[0, "nationality"], Any[0, "birth city"], Any[0, "birth year"], Any[1, "id"], Any[1, "msid"], Any[1, "cid"], Any[2, "id"], Any[2, "msid"], Any[2, "aid"], Any[2, "role"], Any[3, "gid"], Any[3, "genre"], Any[4, "id"], Any[4, "msid"], Any[4, "gid"], Any[5, "id"], Any[5, "name"], Any[5, "country code"], Any[6, "did"], Any[6, "gender"], Any[6, "name"], Any[6, "nationality"], Any[6, "birth city"], Any[6, "birth year"], Any[7, "pid"], Any[7, "gender"], Any[7, "name"], Any[7, "nationality"], Any[7, "birth city"], Any[7, "birth year"], Any[8, "id"], Any[8, "msid"], Any[8, "did"], Any[9, "id"], Any[9, "keyword"], Any[10, "id"], Any[10, "msid"], Any[10, "pid"], Any[11, "mid"], Any[11, "title"], Any[11, "release year"], Any[11, "title aka"], Any[11, "budget"], Any[12, "id"], Any[12, "msid"], Any[12, "kid"], Any[13, "sid"], Any[13, "title"], Any[13, "release year"], Any[13, "num of seasons"], Any[13, "num of episodes"], Any[13, "title aka"], Any[13, "budget"], Any[14, "wid"], Any[14, "gender"], Any[14, "name"], Any[14, "nationality"], Any[14, "num of episodes"], Any[14, "birth city"], Any[14, "birth year"], Any[15, "id"], Any[15, "msid"], Any[15, "wid"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "aid"], Any[0, "gender"], Any[0, "name"], Any[0, "nationality"], Any[0, "birth city"], Any[0, "birth year"], Any[1, "id"], Any[1, "msid"], Any[1, "cid"], Any[2, "id"], Any[2, "msid"], Any[2, "aid"], Any[2, "role"], Any[3, "gid"], Any[3, "genre"], Any[4, "id"], Any[4, "msid"], Any[4, "gid"], Any[5, "id"], Any[5, "name"], Any[5, "country code"], Any[6, "did"], Any[6, "gender"], Any[6, "name"], Any[6, "nationality"], Any[6, "birth city"], Any[6, "birth year"], Any[7, "pid"], Any[7, "gender"], Any[7, "name"], Any[7, "nationality"], Any[7, "birth city"], Any[7, "birth year"], Any[8, "id"], Any[8, "msid"], Any[8, "did"], Any[9, "id"], Any[9, "keyword"], Any[10, "id"], Any[10, "msid"], Any[10, "pid"], Any[11, "mid"], Any[11, "title"], Any[11, "release year"], Any[11, "title aka"], Any[11, "budget"], Any[12, "id"], Any[12, "msid"], Any[12, "kid"], Any[13, "sid"], Any[13, "title"], Any[13, "release year"], Any[13, "num of seasons"], Any[13, "num of episodes"], Any[13, "title aka"], Any[13, "budget"], Any[14, "wid"], Any[14, "gender"], Any[14, "name"], Any[14, "nationality"], Any[14, "num of episodes"], Any[14, "birth city"], Any[14, "birth year"], Any[15, "id"], Any[15, "msid"], Any[15, "wid"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
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
        msid ~ ChooseUniformly(possibilities[:msid])
        aid ~ ChooseUniformly(possibilities[:aid])
        role ~ ChooseUniformly(possibilities[:role])
    end

    @class Genre begin
        gid ~ Unmodeled()
        genre ~ ChooseUniformly(possibilities[:genre])
    end

    @class Classification begin
        id ~ Unmodeled()
        msid ~ ChooseUniformly(possibilities[:msid])
        gid ~ ChooseUniformly(possibilities[:gid])
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
        msid ~ ChooseUniformly(possibilities[:msid])
        did ~ ChooseUniformly(possibilities[:did])
    end

    @class Keyword begin
        id ~ Unmodeled()
        keyword ~ ChooseUniformly(possibilities[:keyword])
    end

    @class Made_By begin
        id ~ Unmodeled()
        msid ~ ChooseUniformly(possibilities[:msid])
        pid ~ ChooseUniformly(possibilities[:pid])
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
        msid ~ ChooseUniformly(possibilities[:msid])
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
        msid ~ ChooseUniformly(possibilities[:msid])
        wid ~ ChooseUniformly(possibilities[:wid])
    end

    @class Obs begin
        actor ~ Actor
        copyright ~ Copyright
        cast ~ Cast
        genre ~ Genre
        classification ~ Classification
        company ~ Company
        director ~ Director
        producer ~ Producer
        directed_By ~ Directed_By
        keyword ~ Keyword
        made_By ~ Made_By
        movie ~ Movie
        tags ~ Tags
        tv_Series ~ Tv_Series
        writer ~ Writer
        written_By ~ Written_By
    end
end

query = @query ImdbModel.Obs [
    actor_aid actor.aid
    actor_gender actor.gender
    actor_name actor.name
    actor_nationality actor.nationality
    actor_birth_city actor.birth_city
    actor_birth_year actor.birth_year
    copyright_id copyright.id
    copyright_msid copyright.msid
    copyright_cid copyright.cid
    cast_id cast.id
    cast_role cast.role
    genre_gid genre.gid
    genre genre.genre
    classification_id classification.id
    company_id company.id
    company_name company.name
    company_country_code company.country_code
    director_did director.did
    director_gender director.gender
    director_name director.name
    director_nationality director.nationality
    director_birth_city director.birth_city
    director_birth_year director.birth_year
    producer_pid producer.pid
    producer_gender producer.gender
    producer_name producer.name
    producer_nationality producer.nationality
    producer_birth_city producer.birth_city
    producer_birth_year producer.birth_year
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
    writer_wid writer.wid
    writer_gender writer.gender
    writer_name writer.name
    writer_nationality writer.nationality
    writer_num_of_episodes writer.num_of_episodes
    writer_birth_city writer.birth_city
    writer_birth_year writer.birth_year
    written_by_id written_By.id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
