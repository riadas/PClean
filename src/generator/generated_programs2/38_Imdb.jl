using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("actor_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("actor_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame


subset_size = size(dirty_table, 1)
dirty_table = first(dirty_table, subset_size)
clean_table = first(clean_table, subset_size)

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
cols = Any[Any[-1, "*"], Any[0, "aid"], Any[0, "gender"], Any[0, "name"], Any[0, "nationality"], Any[0, "birth city"], Any[0, "birth year"], Any[1, "id"], Any[1, "msid"], Any[1, "cid"], Any[2, "id"], Any[2, "msid"], Any[2, "aid"], Any[2, "role"], Any[3, "gid"], Any[3, "genre"], Any[4, "id"], Any[4, "msid"], Any[4, "gid"], Any[5, "id"], Any[5, "name"], Any[5, "country code"], Any[6, "did"], Any[6, "gender"], Any[6, "name"], Any[6, "nationality"], Any[6, "birth city"], Any[6, "birth year"], Any[7, "pid"], Any[7, "gender"], Any[7, "name"], Any[7, "nationality"], Any[7, "birth city"], Any[7, "birth year"], Any[8, "id"], Any[8, "msid"], Any[8, "did"], Any[9, "id"], Any[9, "keyword"], Any[10, "id"], Any[10, "msid"], Any[10, "pid"], Any[11, "mid"], Any[11, "title"], Any[11, "release year"], Any[11, "title aka"], Any[11, "budget"], Any[12, "id"], Any[12, "msid"], Any[12, "kid"], Any[13, "sid"], Any[13, "title"], Any[13, "release year"], Any[13, "num of seasons"], Any[13, "num of episodes"], Any[13, "title aka"], Any[13, "budget"], Any[14, "wid"], Any[14, "gender"], Any[14, "name"], Any[14, "nationality"], Any[14, "num of episodes"], Any[14, "birth city"], Any[14, "birth year"], Any[15, "id"], Any[15, "msid"], Any[15, "wid"]]
foreign_keys = map(tup -> cols[tup[1] + 1], Any[Any[11, 8], Any[12, 1], Any[17, 8], Any[18, 14], Any[36, 22], Any[35, 8], Any[41, 28], Any[40, 8], Any[48, 8], Any[66, 57], Any[65, 8]])
column_names_without_foreign_keys = filter(tup -> !(tup in foreign_keys), cols)
matching_columns = []
for col in dirty_columns 
    println(col)
    match_indices = findall(tup -> lowercase(join(split(join(split(tup[2], " "), ""), "_"), "")) == lowercase(join(split(join(split(col, " "), ""), "_"), "")), column_names_without_foreign_keys)
    if length(match_indices) > 0
        push!(matching_columns, column_names_without_foreign_keys[match_indices[1]][2])
    else
        error("matching column not found")
    end
end
column_renaming_dict = Dict(zip(dirty_columns, matching_columns))
column_renaming_dict_reverse = Dict(zip(matching_columns, dirty_columns))

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
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Copyright begin
        msid ~ Unmodeled()
        cid ~ ChooseUniformly(possibilities[:cid])
    end

    @class Cast begin
        copyright ~ Copyright
        actor ~ Actor
        role ~ ChooseUniformly(possibilities[:role])
    end

    @class Genre begin
        genre ~ ChooseUniformly(possibilities[:genre])
    end

    @class Classification begin
        copyright ~ Copyright
        genre ~ Genre
    end

    @class Company begin
        name ~ ChooseUniformly(possibilities[:name])
        country_code ~ ChooseUniformly(possibilities[:country_code])
    end

    @class Director begin
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Producer begin
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Directed_by begin
        copyright ~ Copyright
        director ~ Director
    end

    @class Keyword begin
        keyword ~ ChooseUniformly(possibilities[:keyword])
    end

    @class Made_by begin
        copyright ~ Copyright
        producer ~ Producer
    end

    @class Movie begin
        title ~ ChooseUniformly(possibilities[:title])
        release_year ~ ChooseUniformly(possibilities[:release_year])
        title_aka ~ ChooseUniformly(possibilities[:title_aka])
        budget ~ ChooseUniformly(possibilities[:budget])
    end

    @class Tags begin
        copyright ~ Copyright
        kid ~ ChooseUniformly(possibilities[:kid])
    end

    @class Tv_series begin
        title ~ ChooseUniformly(possibilities[:title])
        release_year ~ ChooseUniformly(possibilities[:release_year])
        num_of_seasons ~ ChooseUniformly(possibilities[:num_of_seasons])
        num_of_episodes ~ ChooseUniformly(possibilities[:num_of_episodes])
        title_aka ~ ChooseUniformly(possibilities[:title_aka])
        budget ~ ChooseUniformly(possibilities[:budget])
    end

    @class Writer begin
        gender ~ ChooseUniformly(possibilities[:gender])
        name ~ ChooseUniformly(possibilities[:name])
        nationality ~ ChooseUniformly(possibilities[:nationality])
        num_of_episodes ~ ChooseUniformly(possibilities[:num_of_episodes])
        birth_city ~ ChooseUniformly(possibilities[:birth_city])
        birth_year ~ ChooseUniformly(possibilities[:birth_year])
    end

    @class Written_by begin
        id ~ Unmodeled()
        copyright ~ Copyright
        writer ~ Writer
    end

    @class Obs begin
        cast ~ Cast
        classification ~ Classification
        company ~ Company
        directed_by ~ Directed_by
        keyword ~ Keyword
        made_by ~ Made_by
        movie ~ Movie
        tags ~ Tags
        tv_series ~ Tv_series
        written_by ~ Written_by
    end
end

query = @query ImdbModel.Obs [
    actor_gender cast.actor.gender
    actor_name cast.actor.name
    actor_nationality cast.actor.nationality
    actor_birth_city cast.actor.birth_city
    actor_birth_year cast.actor.birth_year
    copyright_msid cast.copyright.msid
    copyright_cid cast.copyright.cid
    cast_role cast.role
    genre classification.genre.genre
    company_name company.name
    company_country_code company.country_code
    director_gender directed_by.director.gender
    director_name directed_by.director.name
    director_nationality directed_by.director.nationality
    director_birth_city directed_by.director.birth_city
    director_birth_year directed_by.director.birth_year
    producer_gender made_by.producer.gender
    producer_name made_by.producer.name
    producer_nationality made_by.producer.nationality
    producer_birth_city made_by.producer.birth_city
    producer_birth_year made_by.producer.birth_year
    keyword keyword.keyword
    movie_title movie.title
    movie_release_year movie.release_year
    movie_title_aka movie.title_aka
    movie_budget movie.budget
    tags_kid tags.kid
    tv_series_title tv_series.title
    tv_series_release_year tv_series.release_year
    tv_series_num_of_seasons tv_series.num_of_seasons
    tv_series_num_of_episodes tv_series.num_of_episodes
    tv_series_title_aka tv_series.title_aka
    tv_series_budget tv_series.budget
    writer_gender written_by.writer.gender
    writer_name written_by.writer.name
    writer_nationality written_by.writer.nationality
    writer_num_of_episodes written_by.writer.num_of_episodes
    writer_birth_city written_by.writer.birth_city
    writer_birth_year written_by.writer.birth_year
    written_by_id written_by.id
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
