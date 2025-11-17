summarise_pct_responses <- function(params, column_config) {
    pct_responses <- purrr::map_dfr(params, function(param) {
        get_pct_responses(
            param$data,
            param$target_columns,
            denominator = param$denominator,
            dplyr::coalesce(param$subset_hoh_and_or_adult, TRUE)
        ) |>
            dplyr::mutate(stage = param$stage, .before = "column") |>
            dplyr::mutate(denominator = param$denominator, .after = "n")
    })

    responses_entry <- pct_responses |>
        dplyr::filter(stage == "entry") |>
        dplyr::select(-stage)

    responses_exit <- pct_responses |>
        dplyr::filter(stage == "exit") |>
        dplyr::select(-stage) |>
        dplyr::mutate(
            column = dplyr::case_when(
                column == "destination" ~ "living_situation",
                TRUE ~ column
            )
        )

    entry <- column_config |>
        dplyr::filter(group == "Entry") |>
        dplyr::left_join(
            y = responses_entry,
            by = "column"
        )

    entry_exit <- column_config |>
        dplyr::filter(group == "Entry & Exit") |>
        dplyr::left_join(
            y = responses_entry,
            by = "column"
        ) |>
        dplyr::left_join(
            y = responses_exit,
            by = "column",
            suffix = c("_entry", "_exit")
        )

    exit <- column_config |>
        dplyr::filter(group == "Exit") |>
        dplyr::left_join(
            y = responses_exit,
            by = "column"
        )

    list(
        entry = entry,
        entry_exit = entry_exit,
        exit = exit
    )
}

get_pct_responses <- function(
    data,
    target_columns,
    subset_hoh_and_or_adult = TRUE,
    denominator,
    no_response_values = c(
        "Client doesn't know",
        "Client prefers not to answer",
        "Data not collected",
        "Missing"
    )
) {
    if (subset_hoh_and_or_adult == TRUE) {
        data <- data |>
            dplyr::filter(is_hoh_and_or_adult == "Yes")
    }

    data |>
        dplyr::mutate(
            dplyr::across(
                .cols = dplyr::all_of(target_columns),
                .fns = function(column) {
                    dplyr::case_when(
                        column %in% no_response_values ~ "Missing response",
                        TRUE ~ "Client provided a response"
                    )
                }
            )
        ) |>
        tidyr::pivot_longer(
            cols = dplyr::all_of(target_columns),
            names_to = "column",
            values_to = "response_status"
        ) |>
        dplyr::count(column, response_status) |>
        dplyr::mutate(pct = n / denominator, .by = "column") |>
        dplyr::filter(response_status == "Client provided a response") |>
        dplyr::select(-response_status)
}

# Get denominators
n_clients_entry <- nrow(entries_in_period)

n_clients_exit <- nrow(exits_in_period)

n_hoh_and_or_adult_entry <- data_hoh_and_or_adult |>
    dplyr::semi_join(
        entries_in_period,
        by = c("enrollment_id", "personal_id", "organization_id")
    ) |>
    dplyr::filter(is_hoh_and_or_adult == "Yes") |>
    nrow()

n_hoh_and_or_adult_exit <- data_hoh_and_or_adult |>
    dplyr::semi_join(
        exits_in_period,
        by = c("enrollment_id", "personal_id", "organization_id")
    ) |>
    dplyr::filter(is_hoh_and_or_adult == "Yes") |>
    nrow()

education_columns <- c(
    "last_grade_completed",
    "school_status"
)

health_columns <- c(
    "general_health_status",
    "dental_health_status",
    "mental_health_status",
    "pregnancy_status"
)

exit_columns <- c(
    "exchange_for_sex",
    "work_place_violence_threats",
    "workplace_promise_difference",
    "project_completion_status",
    "counseling_received",
    "destination_safe_client"
)

# Create list input
summary_params <- list(
    # Enrollment
    ## Social Security Number
    list(
        data = data_enrollment_client,
        target_columns = "ssn_data_quality",
        subset_hoh_and_or_adult = FALSE,
        denominator = n_clients_entry,
        no_response_values = c(
            "Approximate or partial SSN reported",
            "Client doesn't know",
            "Client prefers not to answer",
            "Data not collected",
            "Missing"
        ),
        stage = "entry"
    ),
    ## Date of Birth
    list(
        data = data_enrollment_client,
        target_columns = "age_grouped",
        subset_hoh_and_or_adult = FALSE,
        denominator = n_clients_entry,
        no_response_values = "Missing",
        stage = "entry"
    ),
    ## Relationship to Head of Household
    list(
        data = data_enrollment_client,
        target_columns = "relationship_to_ho_h",
        subset_hoh_and_or_adult = FALSE,
        denominator = n_clients_entry,
        stage = "entry"
    ),
    ## Living Situation
    list(
        data = data_enrollment_client,
        target_columns = c(
            "living_situation",
            "referral_source",
            "former_ward_child_welfare",
            "former_ward_juvenile_justice"
        ),
        denominator = n_hoh_and_or_adult_entry,
        stage = "entry"
    ),
    # Health Conditions and Disabilities
    ## Physical Disability
    list(
        data = data_physical_disability_entry,
        target_columns = "physical_disability",
        denominator = n_clients_entry,
        subset_hoh_and_or_adult = FALSE,
        stage = "entry"
    ),
    list(
        data = data_physical_disability_exit,
        target_columns = "physical_disability",
        denominator = n_clients_exit,
        subset_hoh_and_or_adult = FALSE,
        stage = "exit"
    ),
    ## Developmental Disability
    list(
        data = data_developmental_disability_entry,
        target_columns = "developmental_disability",
        denominator = n_clients_entry,
        subset_hoh_and_or_adult = FALSE,
        stage = "entry"
    ),
    list(
        data = data_developmental_disability_exit,
        target_columns = "developmental_disability",
        denominator = n_clients_exit,
        subset_hoh_and_or_adult = FALSE,
        stage = "exit"
    ),
    ## Chronic Health Condition
    list(
        data = data_chronic_health_condition_entry,
        target_columns = "chronic_health_condition",
        denominator = n_clients_entry,
        subset_hoh_and_or_adult = FALSE,
        stage = "entry"
    ),
    list(
        data = data_chronic_health_condition_exit,
        target_columns = "chronic_health_condition",
        denominator = n_clients_exit,
        subset_hoh_and_or_adult = FALSE,
        stage = "exit"
    ),
    ## HIV/AIDS
    list(
        data = data_hiv_aids_entry,
        target_columns = "hiv_aids",
        denominator = n_clients_entry,
        subset_hoh_and_or_adult = FALSE,
        stage = "entry"
    ),
    list(
        data = data_hiv_aids_exit,
        target_columns = "hiv_aids",
        denominator = n_clients_exit,
        subset_hoh_and_or_adult = FALSE,
        stage = "exit"
    ),
    ## Mental Health Disorder
    list(
        data = data_mental_health_disorder_entry,
        target_columns = "mental_health_disorder",
        denominator = n_clients_entry,
        subset_hoh_and_or_adult = FALSE,
        stage = "entry"
    ),
    list(
        data = data_mental_health_disorder_exit,
        target_columns = "mental_health_disorder",
        denominator = n_clients_exit,
        subset_hoh_and_or_adult = FALSE,
        stage = "exit"
    ),
    ## Substance Use Disorder
    list(
        data = data_substance_use_disorder_entry,
        target_columns = "substance_use_disorder",
        denominator = n_clients_entry,
        subset_hoh_and_or_adult = FALSE,
        stage = "entry"
    ),
    list(
        data = data_substance_use_disorder_exit,
        target_columns = "substance_use_disorder",
        denominator = n_clients_exit,
        subset_hoh_and_or_adult = FALSE,
        stage = "exit"
    ),
    # Domestic Violence
    list(
        data = data_domestic_violence_entry,
        target_columns = "domestic_violence_survivor",
        denominator = n_hoh_and_or_adult_entry,
        stage = "entry"
    ),
    # Education
    list(
        data = data_education_entry,
        target_columns = education_columns,
        denominator = n_hoh_and_or_adult_entry,
        stage = "entry"
    ),
    list(
        data = data_education_exit,
        target_columns = education_columns,
        denominator = n_hoh_and_or_adult_exit,
        stage = "exit"
    ),
    # Employment
    list(
        data = data_employment_entry,
        target_columns = "employed",
        denominator = n_hoh_and_or_adult_entry,
        stage = "entry"
    ),
    list(
        data = data_employment_exit,
        target_columns = "employed",
        denominator = n_hoh_and_or_adult_exit,
        stage = "exit"
    ),
    # Health Status
    list(
        data = data_health_entry,
        target_columns = health_columns,
        denominator = n_hoh_and_or_adult_entry,
        stage = "entry"
    ),
    list(
        data = data_health_exit,
        target_columns = health_columns,
        denominator = n_hoh_and_or_adult_exit,
        stage = "exit"
    ),
    # Exit
    list(
        data = data_exit,
        target_columns = exit_columns,
        denominator = n_hoh_and_or_adult_exit,
        stage = "exit"
    ),
    list(
        data = data_exit,
        target_columns = "destination_safe_worker",
        no_response_values = c("Worker does not know", "Missing"),
        denominator = n_hoh_and_or_adult_exit,
        stage = "exit"
    ),
    list(
        data = data_exit,
        target_columns = "destination",
        subset_hoh_and_or_adult = FALSE,
        denominator = n_clients_exit,
        stage = "exit"
    )
)

column_config <- list(
    ssn_data_quality = list(
        group = "Entry",
        label = "SSN Data Quality"
    ),
    age_grouped = list(
        group = "Entry",
        label = "Age"
    ),
    relationship_to_ho_h = list(
        group = "Entry",
        label = "Relationship To Head of Household"
    ),
    former_ward_child_welfare = list(
        group = "Entry",
        label = "Former Ward Child Welfare"
    ),
    former_ward_juvenile_justice = list(
        group = "Entry",
        label = "Former Ward Juvenile Justice"
    ),
    living_situation = list(
        group = "Entry & Exit",
        label = "Living Situation"
    ),
    referral_source = list(
        group = "Entry",
        label = "Referral Source"
    ),
    physical_disability = list(
        group = "Entry & Exit",
        label = "Physical Disability"
    ),
    developmental_disability = list(
        group = "Entry & Exit",
        label = "Developmental Disability"
    ),
    chronic_health_condition = list(
        group = "Entry & Exit",
        label = "Chronic Health Condition"
    ),
    hiv_aids = list(
        group = "Entry & Exit",
        label = "HIV/AIDS"
    ),
    mental_health_disorder = list(
        group = "Entry & Exit",
        label = "Mental Health Disorder"
    ),
    substance_use_disorder = list(
        group = "Entry & Exit",
        label = "Substance Use Disorder"
    ),
    domestic_violence_survivor = list(
        group = "Entry",
        label = "Domestic Violence Survivor"
    ),
    last_grade_completed = list(
        group = "Entry & Exit",
        label = "Last Grade Completed"
    ),
    school_status = list(
        group = "Entry & Exit",
        label = "School Status"
    ),
    employed = list(
        group = "Entry & Exit",
        label = "Employed"
    ),
    dental_health_status = list(
        group = "Entry & Exit",
        label = "Dental Health Status"
    ),
    general_health_status = list(
        group = "Entry & Exit",
        label = "General Health Status"
    ),
    mental_health_status = list(
        group = "Entry & Exit",
        label = "Mental Health Status"
    ),
    pregnancy_status = list(
        group = "Entry & Exit",
        label = "Pregnancy Status"
    ),
    counseling_received = list(
        group = "Exit",
        label = "Counseling Received"
    ),
    destination_safe_client = list(
        group = "Exit",
        label = "Destination Safe Client"
    ),
    exchange_for_sex = list(
        group = "Exit",
        label = "Exchange For Sex"
    ),
    project_completion_status = list(
        group = "Exit",
        label = "Project Completion Status"
    ),
    work_place_violence_threats = list(
        group = "Exit",
        label = "Work Place Violence Threats"
    ),
    workplace_promise_difference = list(
        group = "Exit",
        label = "Workplace Promise Difference"
    ),
    destination_safe_worker = list(
        group = "Exit",
        label = "Destination Safe Worker"
    )
) |>
    dplyr::bind_rows(.id = "column")
