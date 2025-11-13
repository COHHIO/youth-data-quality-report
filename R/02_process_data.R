# Read data
dm <- readRDS("data/dm_20251013.rds")

# Get entries and exits in period
entries_in_period <- dm$enrollment |>
    dplyr::select(enrollment_id, personal_id, organization_id, entry_date) |>
    dplyr::filter(
        entry_date >= period_start_date & entry_date <= period_end_date
    )

exits_in_period <- dm$exit |>
    dplyr::select(enrollment_id, personal_id, organization_id, exit_date) |>
    dplyr::filter(
        exit_date >= period_start_date & exit_date <= period_end_date
    )

# Process all enrollment client data
processed_enrollment_client <- dm$enrollment |>
    dplyr::left_join(
        y = dm$client,
        by = c("personal_id", "organization_id")
    ) |>
    dplyr::mutate(
        age = lubridate::time_length(
            difftime(entry_date, dob),
            "years"
        ) |>
            floor(),
        age_grouped = dplyr::case_when(
            age >= 25 ~ "25+",
            age >= 18 & age <= 24 ~ "18-24",
            age >= 14 & age <= 17 ~ "14-17",
            age >= 6 & age <= 13 ~ "6-13",
            age >= 0 & age <= 5 ~ "0-5",
            TRUE ~ "Missing"
        ) |>
            factor(
                levels = c("Missing", "0-5", "6-13", "14-17", "18-24", "25+")
            ),
        is_adult = dplyr::case_when(
            age >= 18 ~ "Yes",
            age < 18 ~ "No",
            TRUE ~ "Unknown"
        ) |>
            factor(levels = c("Yes", "No", "Unknown")),
        is_hoh = dplyr::case_when(
            relationship_to_ho_h == "Self (head of household)" ~ "Yes",
            relationship_to_ho_h %in%
                c("Data not collected", "Missing") ~ "Unknown",
            TRUE ~ "No"
        ) |>
            factor(levels = c("Yes", "No", "Unknown")),
        is_hoh_and_or_adult = dplyr::case_when(
            is_adult == "Yes" | is_hoh == "Yes" ~ "Yes",
            is_adult == "No" & is_hoh == "No" ~ "No",
            TRUE ~ "Unknown"
        ) |>
            factor(levels = c("Yes", "No", "Unknown")),
        ssn_data_quality = factor(
            ssn_data_quality,
            levels = c(
                "Full SSN reported",
                "Approximate or partial SSN reported",
                "Client doesn't know",
                "Client prefers not to answer",
                "Data not collected"
            )
        ),
        ssn_length = dplyr::case_when(
            ssn == "Missing" ~ "No Digits",
            (stringr::str_length(ssn) / 4) == 9 ~ "9 Digits",
            TRUE ~ "1 to 8 Digits"
        ) |>
            factor(levels = c("9 Digits", "1 to 8 Digits", "No Digits"))
    )

# Head of Household and Adult identification
data_hoh_and_or_adult <- processed_enrollment_client |>
    dplyr::select(
        enrollment_id,
        personal_id,
        organization_id,
        is_adult,
        is_hoh,
        is_hoh_and_or_adult
    )

# Process period enrollment client data
data_enrollment_client <- processed_enrollment_client |>
    dplyr::semi_join(
        entries_in_period,
        by = c("enrollment_id", "personal_id", "organization_id")
    )

# Process education data
data_education_entry <- process_program_specific_data_element(
    data_element = dm$education,
    enrollments = entries_in_period,
    stage = "Project start",
    data_hoh_and_or_adult = data_hoh_and_or_adult
)

data_education_exit <- process_program_specific_data_element(
    data_element = dm$education,
    enrollments = exits_in_period,
    stage = "Project exit",
    data_hoh_and_or_adult = data_hoh_and_or_adult
)

# Process employment data
data_employment_entry <- process_program_specific_data_element(
    data_element = dm$employment,
    enrollments = entries_in_period,
    stage = "Project start",
    data_hoh_and_or_adult = data_hoh_and_or_adult
)

data_employment_exit <- process_program_specific_data_element(
    data_element = dm$employment,
    enrollments = exits_in_period,
    stage = "Project exit",
    data_hoh_and_or_adult = data_hoh_and_or_adult
)

# Process health data
data_health_entry <- process_program_specific_data_element(
    data_element = dm$health,
    enrollments = entries_in_period,
    stage = "Project start",
    data_hoh_and_or_adult = data_hoh_and_or_adult
)

data_health_exit <- process_program_specific_data_element(
    data_element = dm$health,
    enrollments = exits_in_period,
    stage = "Project exit",
    data_hoh_and_or_adult = data_hoh_and_or_adult
)

# Process health conditions and disabilities data
data_health_conditions_and_disabilites <- dm$disabilities |>
    dplyr::distinct() |>
    # Complete missing responses
    dplyr::mutate(
        disability_response = dplyr::case_when(
            disability_response %in%
                c(
                    "Yes",
                    "No",
                    "Alcohol use disorder",
                    "Drug use disorder",
                    "Both alcohol and drug use disorders"
                ) ~ "Client provided a response",
            TRUE ~ disability_response
        ) |>
            factor(
                levels = c(
                    "Client provided a response",
                    "Client doesn't know",
                    "Client prefers not to answer",
                    "Data not collected",
                    "Missing"
                )
            )
    ) |>
    tidyr::pivot_wider(
        names_from = disability_type,
        values_from = disability_response
    ) |>
    dplyr::mutate(
        dplyr::across(
            dplyr::where(is.factor),
            ~ tidyr::replace_na(.x, "Missing")
        )
    ) |>
    tidyr::pivot_longer(
        cols = c(
            "Physical Disability",
            "Developmental Disability",
            "Chronic Health Condition",
            "HIV/AIDS",
            "Mental Health Disorder",
            "Substance Use Disorder"
        ),
        names_to = "disability_type",
        values_to = "disability_response"
    )


data_health_conditions_and_disabilites_entry <- process_program_specific_data_element(
    data_element = data_health_conditions_and_disabilites,
    enrollments = entries_in_period,
    stage = "Project start"
)

data_health_conditions_and_disabilites_exit <- process_program_specific_data_element(
    data_element = data_health_conditions_and_disabilites,
    enrollments = exits_in_period,
    stage = "Project exit"
)
