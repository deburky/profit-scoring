{{ config(materialized='table') }}

with bondora_loan_dataset as (

    select *
    from dbt_playground.bondora_loan_dataset

),

dbt_transformation as (
    select
        "PartyId",
        "LoanId",
        coalesce("IncomeFromSocialWelfare", 0) as "IncomeFromSocialWelfare", 
        coalesce("PrincipalPaymentsMade", 0) as "PrincipalPaymentsMade", 
        coalesce("RefinanceLiabilities", 0) as "RefinanceLiabilities", 
        coalesce("BidsPortfolioManager", 0) as "BidsPortfolioManager", 
        coalesce("InterestAndPenaltyPaymentsMade", 0) as "InterestAndPenaltyPaymentsMade", 
        coalesce("ApplicationSignedHour", 0) as "ApplicationSignedHour", 
        coalesce("IncomeOther", 0) as "IncomeOther", 
        coalesce("MonthlyPaymentDay", 0) as "MonthlyPaymentDay", 
        coalesce("ExistingLiabilities", 0) as "ExistingLiabilities", 
        coalesce("DebtToIncome", 0) as "DebtToIncome", 
        coalesce("IncomeTotal", 0) as "IncomeTotal", 
        coalesce("FreeCash", 0) as "FreeCash", 
        coalesce("PreviousEarlyRepaymentsCountBeforeLoan", 0) as "PreviousEarlyRepaymentsCountBeforeLoan", 
        coalesce("IncomeFromFamilyAllowance", 0) as "IncomeFromFamilyAllowance", 
        coalesce("PreviousRepaymentsBeforeLoan", 0) as "PreviousRepaymentsBeforeLoan", 
        coalesce("BidsManual", 0) as "BidsManual", 
        coalesce("IncomeFromPrincipalEmployer", 0) as "IncomeFromPrincipalEmployer", 
        coalesce("AmountOfPreviousLoansBeforeLoan", 0) as "AmountOfPreviousLoansBeforeLoan", 
        coalesce("PreviousEarlyRepaymentsBefoleLoan", 0) as "PreviousEarlyRepaymentsBefoleLoan", 
        case 
            when "NrOfDependants" = '10Plus' then 10
            when "NrOfDependants" = '1.0' then 1
            when "NrOfDependants" = '2.0' then 2
            when "NrOfDependants" = '3.0' then 3
            when "NrOfDependants" = '4.0' then 4
            when "NrOfDependants" = '5.0' then 5
            when "NrOfDependants" = '7.0' then 7
            when "NrOfDependants" = '8.0' then 8
            when "NrOfDependants" = '0.0' then 0
            when "NrOfDependants" is null then 0
            else cast("NrOfDependants" as INTEGER)
        end as "NrOfDependants",
        coalesce("ApplicationSignedWeekday", 0) as "ApplicationSignedWeekday", 
        coalesce("IncomeFromPension", 0) as "IncomeFromPension", 
        coalesce("BidsApi", 0) as "BidsApi",
        coalesce("IncomeFromLeavePay", 0) as "IncomeFromLeavePay", 
        coalesce("LiabilitiesTotal", 0) as "LiabilitiesTotal", 
        coalesce("NoOfPreviousLoansBeforeLoan", 0) as "NoOfPreviousLoansBeforeLoan", 
        coalesce("Age", 0) as "Age",
        coalesce("IncomeFromChildSupport", 0) as "IncomeFromChildSupport",
        case 
            when "NewCreditCustomer" = True 
            then 'Existing_credit_customer'
            else 'New_credit_Customer' 
        end as "NewCreditCustomer",
        case
            when "VerificationType" = 0.0
            then 'Not_set'
            when "VerificationType" is null
            then 'Not_set'
            when "VerificationType" = 1.0
            then 'Income_unverified'
            when "VerificationType" = 2.0
            then 'Income_unverified_crossref_phone'
            when "VerificationType" = 3.0
            then 'Income_verified'
            when "VerificationType" = 4.0
            then 'Income_expenses_verified'
        end as "VerificationType",
        case
            when "LanguageCode" = 1
            then 'Estonian'
            when "LanguageCode" = 2
            then 'Others'
            when "LanguageCode" = 3
            then 'Russian'
            when "LanguageCode" = 4
            then 'Finnish'
            when "LanguageCode" = 5
            then 'Others'
            when "LanguageCode" = 6
            then 'Spanish'
            when "LanguageCode" = 9
            then 'Others'
            when "LanguageCode" = 7
            then 'Others'
            when "LanguageCode" = 8
            then 'Others'
            when "LanguageCode" = 10
            then 'Others'
            when "LanguageCode" = 13
            then 'Others'
            when "LanguageCode" = 15
            then 'Others'
            when "LanguageCode" = 19
            then 'Others'
            when "LanguageCode" = 21
            then 'Others'
            when "LanguageCode" = 22
            then 'Others'
            else 'Not_specified'
        end as "LanguageCode",
        case
            when "Gender" = 0.0
            then 'Male'
            when "Gender" = 1.0
            then 'Female'
            when "Gender" = 2.0
            then 'Unknown'
            when "Gender" is null
            then 'Unknown'
        end as "Gender",
        case
            when "UseOfLoan" = -1
            then 'Not_set'
            when "UseOfLoan" = 0
            then 'Loan_consolidation'
            when "UseOfLoan" = 1
            then 'Real_estate'
            when "UseOfLoan" = 2
            then 'Home_improvement'
            when "UseOfLoan" = 3
            then 'Business'
            when "UseOfLoan" = 4
            then 'Education'
            when "UseOfLoan" = 5
            then 'Travel'
            when "UseOfLoan" = 6
            then 'Vehicle'
            when "UseOfLoan" = 7
            then 'Other'
            when "UseOfLoan" = 8
            then 'Health'
            when "UseOfLoan" = 101
            then 'Working_capital_financing'
            when "UseOfLoan" = 102
            then 'Purchase_of_machinery_equipment'
            when "UseOfLoan" = 104
            then 'Accounts_receivable_financing'
            when "UseOfLoan" = 106
            then 'Construction_finance'
            when "UseOfLoan" = 107
            then 'Acquisition_of_stocks'
            when "UseOfLoan" = 108
            then 'Acquisition_of_real_estate'
            when "UseOfLoan" = 110
            then 'Other_business'
            else 'Not_specified'
        end as "UseOfLoan",
        case
            when "Education" = 1.0
            then 'Primary'
            when "Education" = 2.0
            then 'Basic'
            when "Education" = 3.0
            then 'Vocational'
            when "Education" = 4.0
            then 'Secondary'
            when "Education" = 5.0
            then 'Higher'
            when "Education" = -1.0
            then 'Not_present'
            when "Education" = 0.0
            then 'Not_present'
            else 'Not_present'
        end as "Education",
        case
            when "MaritalStatus" = 1.0
            then 'Married'
            when "MaritalStatus" = 2.0
            then 'Cohabitant'
            when "MaritalStatus" = 3.0
            then 'Single'
            when "MaritalStatus" = 4.0
            then 'Divorced'
            when "MaritalStatus" = 5.0
            then 'Widow'
            when "MaritalStatus" = -1.0
            then 'Not_specified'
            when "MaritalStatus" = 0.0
            then 'Not_specified'
            else 'Not_specified'
        end as "MaritalStatus",
        case
            when "EmploymentStatus" = 1.0
            then 'Unemployed'
            when "EmploymentStatus" = 2.0
            then 'Partially'
            when "EmploymentStatus" = 3.0
            then 'Fully'
            when "EmploymentStatus" = 4.0
            then 'Self_employed'
            when "EmploymentStatus" = 5.0
            then 'Entrepreneur'
            when "EmploymentStatus" = 6.0
            then 'Retiree'
            when "EmploymentStatus" = -1.0
            then 'Not_specified'
            when "EmploymentStatus" = 0.0
            then 'Not_specified'
            else 'Not_specified'
        end as "EmploymentStatus",
        "EmploymentDurationCurrentEmployer",
        case
            when "OccupationArea" = 1
            then 'Other'
            when "OccupationArea" = 2
            then 'Mining'
            when "OccupationArea" = 3
            then 'Processing'
            when "OccupationArea" = 4
            then 'Energy'
            when "OccupationArea" = 5
            then 'Utilities'
            when "OccupationArea" = 6
            then 'Construction'
            when "OccupationArea" = 7
            then 'Retail_and_wholesale'
            when "OccupationArea" = 8
            then 'Transport_and_warehousing'
            when "OccupationArea" = 9
            then 'Hospitality_and_catering'
            when "OccupationArea" = 10
            then 'Info_and_telecom'
            when "OccupationArea" = 11
            then 'Finance_and_insurance'
            when "OccupationArea" = 12
            then 'Real_estate'
            when "OccupationArea" = 13
            then 'Research'
            when "OccupationArea" = 14
            then 'Administrative'
            when "OccupationArea" = 15
            then 'Civil_service_and_military'
            when "OccupationArea" = 16
            then 'Education'
            when "OccupationArea" = 17
            then 'Healthcare_and_social_help'
            when "OccupationArea" = 18
            then 'Art_and_entertainment'
            when "OccupationArea" = 19
            then 'Agriculture_forestry_and_fishing'
            when "OccupationArea" = -1.0
            then 'Not_specified'
            when "OccupationArea" = 0.0
            then 'Not_specified'
            else 'Not_specified'
        end as "OccupationArea",
        case
            when "HomeOwnershipType" = -1.0
            then 'Not_specified'
            when "HomeOwnershipType" is null
            then 'Not_specified'
            when "HomeOwnershipType" = 0.0
            then 'Homeless'
            when "HomeOwnershipType" = 1.0
            then 'Owner'
            when "HomeOwnershipType" = 2.0
            then 'Living_with_parents'
            when "HomeOwnershipType" = 3.0
            then 'Tenant_pre_furnished_property'
            when "HomeOwnershipType" = 4.0
            then 'Tenant_unfurnished_property'
            when "HomeOwnershipType" = 5.0
            then 'Council_house'
            when "HomeOwnershipType" = 6.0
            then 'Joint_tenant'
            when "HomeOwnershipType" = 7.0
            then 'Joint_ownership'
            when "HomeOwnershipType" = 8.0
            then 'Mortgage'
            when "HomeOwnershipType" = 9.0
            then 'Owner_with_encumbrance'
            when "HomeOwnershipType" = 10.0
            then 'Other'
            else 'Not_specified'
        end as "HomeOwnershipType",
        case
            when "Restructured" = false
            then 'No'
            when "Restructured" = true
            then 'Yes'
            else 'Not_specified'
        end as "Restructured",
        coalesce("CreditScoreEsMicroL", 'UNKNOWN') as 
"CreditScoreEsMicroL"
    from bondora_loan_dataset
)

select * from dbt_transformation


