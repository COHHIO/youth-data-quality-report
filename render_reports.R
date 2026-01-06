config <- yaml::read_yaml("_config.yml")
purrr::walk(.x = config$organizations, function(grantee) {
    quarto::quarto_render(
        input = "report.qmd",
        output_format = "typst",
        output_file = paste0("Data Quality Report - ", grantee$name),
        execute_params = list(
            period_start_date = "2025-07-01",
            period_end_date = "2025-09-30",
            grantee = grantee
        )
    )
})

