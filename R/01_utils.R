create_data_quality_table <- function(
    data,
    response_column,
    no_response_values = c(
        "Client doesn't know",
        "Client prefers not to answer",
        "Data not collected",
        "Missing"
    ),
    combine_responses = TRUE,
    group_column = NULL,
    group_column_label = NULL,
    table_title
) {
    if (combine_responses == TRUE) {
        table_data <- data |>
            dplyr::mutate(
                data_quality_status = dplyr::case_when(
                    .data[[response_column]] %in% no_response_values ~ .data[[
                        response_column
                    ]],
                    TRUE ~ "Client provided a response"
                ) |>
                    factor(
                        levels = c(
                            "Client provided a response",
                            no_response_values
                        )
                    )
            )
    } else {
        table_data <- data |>
            dplyr::mutate(
                data_quality_status = .data[[response_column]]
            )
    }

    if (is.null(group_column)) {
        table_data <- table_data |>
            dplyr::count(data_quality_status, name = "Count", .drop = FALSE)
    } else {
        table_data <- table_data |>
            dplyr::count(
                data_quality_status,
                .data[[group_column]],
                .drop = FALSE
            ) |>
            tidyr::pivot_wider(
                names_from = dplyr::all_of(group_column),
                values_from = n
            )

        spanner_columns <- names(table_data)[
            names(table_data) != "data_quality_status"
        ]
    }

    gt <- table_data |>
        gt::gt() |>
        gt::tab_header(title = table_title) |>
        gt::cols_label(
            data_quality_status = "Data Quality Status"
        ) |>
        gt::cols_width(
            data_quality_status ~ gt::pct(55),
            gt::everything() ~ gt::pct(15)
        ) |>
        gt::cols_align(
            align = "left",
            columns = data_quality_status
        )

    if (!is.null(group_column)) {
        gt <- gt |>
            gt::cols_align(
                align = "right",
                columns = dplyr::all_of(spanner_columns)
            ) |>
            gt::tab_spanner(
                label = group_column_label,
                columns = dplyr::all_of(spanner_columns)
            ) |>
            gt::tab_style(
                style = gt::cell_text(v_align = "middle"),
                locations = gt::cells_column_labels(
                    columns = data_quality_status
                )
            )
    }

    gt |>
        gt::tab_options(
            table.font.names = "Roboto"
        )
}

process_program_specific_data_element <- function(
    data_element,
    enrollments,
    stage,
    data_hoh_and_or_adult = NULL
) {
    out <- data_element |>
        dplyr::semi_join(
            enrollments,
            by = c("enrollment_id", "personal_id", "organization_id")
        ) |>
        dplyr::filter(data_collection_stage == stage)

    if (!is.null(data_hoh_and_or_adult)) {
        out <- out |>
            dplyr::left_join(
                y = data_hoh_and_or_adult,
                by = c("enrollment_id", "personal_id", "organization_id")
            )
    }

    out
}

check_record_match <- function(
    data_entry,
    entries,
    data_exit = NULL,
    exits = NULL
) {
    n_entry_data_records_without_enrollment_record <- data_entry |>
        dplyr::anti_join(
            y = entries,
            by = c("enrollment_id", "personal_id", "organization_id")
        ) |>
        nrow()

    if (n_entry_data_records_without_enrollment_record == 0) {
        cat(
            "✅ All records collected at entry match an enrollment record.  \n"
        )
    } else {
        cat(paste0(
            "❌ ",
            n_entry_data_records_without_enrollment_record,
            " records collected at entry don't match an enrollment record.  \n"
        ))
    }

    n_enrollments_without_entry_data_record <- entries |>
        dplyr::anti_join(
            y = data_entry,
            by = c("enrollment_id", "personal_id", "organization_id")
        ) |>
        dplyr::distinct(enrollment_id, personal_id, organization_id) |>
        nrow()

    if (n_enrollments_without_entry_data_record == 0) {
        cat("✅ All enrollments have a data record.  \n")
    } else {
        cat(paste0(
            "❌ ",
            n_enrollments_without_entry_data_record,
            " enrollments are missing a data record.  \n"
        ))
    }

    if (!is.null(data_exit)) {
        n_exit_data_records_without_exit_record <- data_exit |>
            dplyr::anti_join(
                y = exits,
                by = c("enrollment_id", "personal_id", "organization_id")
            ) |>
            nrow()

        if (n_exit_data_records_without_exit_record == 0) {
            cat(paste0(
                "✅ All records collected at exit match an exit record.  \n"
            ))
        } else {
            cat(paste0(
                "❌ ",
                n_exit_data_records_without_exit_record,
                " records collected at exit don't match an exit record.  \n"
            ))
        }

        n_exits_without_exit_data_record <- exits |>
            dplyr::anti_join(
                y = data_exit,
                by = c("enrollment_id", "personal_id", "organization_id")
            ) |>
            dplyr::distinct(enrollment_id, personal_id, organization_id) |>
            nrow()

        if (n_exits_without_exit_data_record == 0) {
            cat(paste0("✅ All exits have an data record.  \n"))
        } else {
            cat(paste0(
                "❌ ",
                n_exits_without_exit_data_record,
                " exits are missing a data record.  \n"
            ))
        }
    }
}
