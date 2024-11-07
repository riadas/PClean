using PClean
using CSV
using DataFrames: DataFrame
using Statistics

# data handling
dirty_table = CSV.File("tv channel_dirty.csv") |> DataFrame
clean_table = CSV.File(replace("tv channel_dirty.csv", "dirty.csv" => "clean.csv")) |> DataFrame

## construct possibilities
column_renaming_dict = Dict(zip(names(dirty_table), map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "series name"], Any[0, "country"], Any[0, "language"], Any[0, "content"], Any[0, "pixel aspect ratio par"], Any[0, "hight definition tv"], Any[0, "pay per view ppv"], Any[0, "package option"], Any[1, "id"], Any[1, "episode"], Any[1, "air date"], Any[1, "rating"], Any[1, "share"], Any[1, "18 49 rating share"], Any[1, "viewers m"], Any[1, "weekly rank"], Any[1, "channel"], Any[2, "id"], Any[2, "title"], Any[2, "directed by"], Any[2, "written by"], Any[2, "original air date"], Any[2, "production code"], Any[2, "channel"]])))
column_renaming_dict_reverse = Dict(zip(map(t -> t[2], Any[Any[-1, "*"], Any[0, "id"], Any[0, "series name"], Any[0, "country"], Any[0, "language"], Any[0, "content"], Any[0, "pixel aspect ratio par"], Any[0, "hight definition tv"], Any[0, "pay per view ppv"], Any[0, "package option"], Any[1, "id"], Any[1, "episode"], Any[1, "air date"], Any[1, "rating"], Any[1, "share"], Any[1, "18 49 rating share"], Any[1, "viewers m"], Any[1, "weekly rank"], Any[1, "channel"], Any[2, "id"], Any[2, "title"], Any[2, "directed by"], Any[2, "written by"], Any[2, "original air date"], Any[2, "production code"], Any[2, "channel"]]), names(dirty_table)))

possibilities = Dict(Symbol(col) => Set() for col in values(column_renaming_dict))
for r in eachrow(dirty_table)
    for col in names(dirty_table)
        if !ismissing(r[col]) 
            push!(possibilities[Symbol(column_renaming_dict[col])], r[col])
        end
    end
end
possibilities = Dict(c => [possibilities[c]...] for c in keys(possibilities))





PClean.@model TvshowModel begin
    @class Tv_Channel begin
        id ~ ChooseUniformly(possibilities[:id])
        series_name ~ ChooseUniformly(possibilities[:series_name])
        country ~ ChooseUniformly(possibilities[:country])
        language ~ ChooseUniformly(possibilities[:language])
        content ~ ChooseUniformly(possibilities[:content])
        pixel_aspect_ratio_par ~ ChooseUniformly(possibilities[:pixel_aspect_ratio_par])
        hight_definition_tv ~ ChooseUniformly(possibilities[:hight_definition_tv])
        pay_per_view_ppv ~ ChooseUniformly(possibilities[:pay_per_view_ppv])
        package_option ~ ChooseUniformly(possibilities[:package_option])
    end

    @class Tv_Series begin
        id ~ Unmodeled()
        episode ~ ChooseUniformly(possibilities[:episode])
        air_date ~ ChooseUniformly(possibilities[:air_date])
        rating ~ ChooseUniformly(possibilities[:rating])
        share ~ ChooseUniformly(possibilities[:share])
        18_49_rating_share ~ ChooseUniformly(possibilities[:18_49_rating_share])
        viewers_m ~ ChooseUniformly(possibilities[:viewers_m])
        weekly_rank ~ ChooseUniformly(possibilities[:weekly_rank])
        channel ~ ChooseUniformly(possibilities[:channel])
    end

    @class Cartoon begin
        id ~ Unmodeled()
        title ~ ChooseUniformly(possibilities[:title])
        directed_by ~ ChooseUniformly(possibilities[:directed_by])
        written_by ~ ChooseUniformly(possibilities[:written_by])
        original_air_date ~ ChooseUniformly(possibilities[:original_air_date])
        production_code ~ ChooseUniformly(possibilities[:production_code])
        channel ~ ChooseUniformly(possibilities[:channel])
    end

    @class Obs begin
        tv_Channel ~ Tv_Channel
        tv_Series ~ Tv_Series
        cartoon ~ Cartoon
    end
end

query = @query TvshowModel.Obs [
    tv_channel_id tv_Channel.id
    tv_channel_series_name tv_Channel.series_name
    tv_channel_country tv_Channel.country
    tv_channel_language tv_Channel.language
    tv_channel_content tv_Channel.content
    tv_channel_pixel_aspect_ratio_par tv_Channel.pixel_aspect_ratio_par
    tv_channel_hight_definition_tv tv_Channel.hight_definition_tv
    tv_channel_pay_per_view_ppv tv_Channel.pay_per_view_ppv
    tv_channel_package_option tv_Channel.package_option
    tv_series_id tv_Series.id
    tv_series_episode tv_Series.episode
    tv_series_air_date tv_Series.air_date
    tv_series_rating tv_Series.rating
    tv_series_share tv_Series.share
    tv_series_18_49_rating_share tv_Series.18_49_rating_share
    tv_series_viewers_m tv_Series.viewers_m
    tv_series_weekly_rank tv_Series.weekly_rank
    cartoon_id cartoon.id
    cartoon_title cartoon.title
    cartoon_directed_by cartoon.directed_by
    cartoon_written_by cartoon.written_by
    cartoon_original_air_date cartoon.original_air_date
    cartoon_production_code cartoon.production_code
]


observations = [ObservedDataset(query, dirty_table)]
config = PClean.InferenceConfig(5, 2; use_mh_instead_of_pg=true)
@time begin 
    tr = initialize_trace(observations, config);
    run_inference!(tr, config)
end

println(evaluate_accuracy(dirty_table, clean_table, tr.tables[:Obs], query))
