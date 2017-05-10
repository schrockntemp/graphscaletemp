import csv
import sys
from enum import Enum

columns = [
    'Provider ID',
    'Hospital Name',
    'Address',
    'City',
    'State',
    'ZIP Code',
    'County Name',
    'Phone Number',
    'Hospital Type',
    'Hospital Ownership',
    'Emergency Services',
    'Meets criteria for meaningful use of EHRs',
    'Hospital overall rating',
    'Hospital overall rating footnote',
    'Mortality national comparison',
    'Mortality national comparison footnote',
    'Safety of care national comparison',
    'Safety of care national comparison footnote',
    'Readmission national comparison',
    'Readmission national comparison footnote',
    'Patient experience national comparison',
    'Patient experience national comparison footnote',
    'Effectiveness of care national comparison',
    'Effectiveness of care national comparison footnote',
    'Timeliness of care national comparison',
    'Timeliness of care national comparison footnote',
    'Efficient use of medical imaging national comparison',
    'Efficient use of medical imaging national comparison footnote',
    'Location',
]

name_lookup = {
    'Provider ID': 'providerID',
    'Hospital Name': 'hospitalName',
    'Address': 'address',
    'City': 'city',
    'State': 'state',
    'ZIP Code': 'zipCode',
    'County Name': 'countyName',
    'Phone Number': 'phoneNumber',
    'Hospital Type': 'hospitalType', #  Enum {'Childrens', 'Acute Care Hospitals', 'Critical Access Hospitals'}
    'Hospital Ownership': 'hospitalOwnership', # Enum {'Government - Hospital District or Authority', 'Voluntary non-profit - Church', 'Government - Local', 'Tribal', 'Government - State', 'Government - Federal', 'Physician', 'Voluntary non-profit - Other', 'Voluntary non-profit - Private', 'Proprietary'}
    'Emergency Services': 'emergencyServices', # Boolean {'false', 'true'}
    'Meets criteria for meaningful use of EHRs': 'meetsMeaningfulUse', # {'', 'true'}
    'Hospital overall rating': 'overallRating', # 1-5 and None
    'Hospital overall rating footnote': 'overallRatingFootnote',
    'Mortality national comparison': 'mortalityComparison', # All comparisons: {'Above the national average', 'Below the national average', 'Not Available', 'Same as the national average'}
    'Mortality national comparison footnote': 'mortalityComparisonFootnote',
    'Safety of care national comparison': 'safetyOfCareComparison', 
    'Safety of care national comparison footnote': 'safetyOfCareComparisonFootnote',
    'Readmission national comparison': 'readmissionComparison',
    'Readmission national comparison footnote': 'readmissionComparisonFootnote',
    'Patient experience national comparison': 'patientExperienceComparison',
    'Patient experience national comparison footnote': 'patientExperienceComparisonFootnote',
    'Effectiveness of care national comparison': 'effectivenessComparison',
    'Effectiveness of care national comparison footnote': 'effectivenessComparisonFootnote',
    'Timeliness of care national comparison': 'timelinessComparison',
    'Timeliness of care national comparison footnote': 'timelinessComparisonFootnote',
    'Efficient use of medical imaging national comparison': 'medicalImaginingComparison',
    'Efficient use of medical imaging national comparison footnote': 'medicalImaginingComparisonFootnote',
    'Location': 'location',
}

class OwnershipEnum(Enum):
    HOSPITAL_DISTRICT_GOVERNMENT = 'HOSPITAL_DISTRICT_GOVERNMENT'
    CHURCH_NONPROFIT = 'CHURCH_NONPROFIT'
    LOCAL_GOVERNMENT = 'LOCAL_GOVERNMENT'
    TRIBAL = 'TRIBAL'
    STATE_GOVERNMENT = 'STATE_GOVERNMENT'
    FEDERAL_GOVERNMENT = 'FEDERAL_GOVERNMENT'
    PHYSICIAN = 'PHYSICIAN'
    PRIVATE_NONPROFIT = 'PRIVATE_NONPROFIT'
    OTHER_NONPROFIT = 'OTHER_NONPROFIT'
    PROPRIETARY = 'PROPRIETARY'

def str_to_ownership_enum(value):
    lookup = {
        'Government - Hospital District or Authority': OwnershipEnum.HOSPITAL_DISTRICT_GOVERNMENT,
        'Voluntary non-profit - Church': OwnershipEnum.CHURCH_NONPROFIT,
        'Government - Local': OwnershipEnum.LOCAL_GOVERNMENT,
        'Tribal': OwnershipEnum.TRIBAL,
        'Government - State': OwnershipEnum.STATE_GOVERNMENT,
        'Government - Federal': OwnershipEnum.FEDERAL_GOVERNMENT,
        'Physician': OwnershipEnum.PHYSICIAN,
        'Voluntary non-profit - Other': OwnershipEnum.OTHER_NONPROFIT,
        'Voluntary non-profit - Private': OwnershipEnum.PRIVATE_NONPROFIT,
        'Proprietary': OwnershipEnum.PROPRIETARY,
    }
    return lookup[value]

def str_to_comparison_enum(value):
    if value == 'Above the national average':
        return ComparisonEnum.ABOVE
    elif value == 'Below the national average':
        return ComparisonEnum.BELOW
    elif value == 'Same as the national average':
        return ComparisonEnum.SAME
    elif value == 'Not Available':
        return ComparisonEnum.NA
    else:
        raise Exception('value not supported: ' + value)

def str_to_boolean(value):
    if value == 'true':
        return True
    elif value in ['', 'false']:
        return False
    else:
        raise Exception('value not supported: ' + value)

def str_to_hospital_type(value):
    lookup = {
        'Childrens': HospitalType.CHILDRENS,
        'Acute Care Hospitals': HospitalType.ACUTE_CARE,
        'Critical Access Hospitals': HospitalType.CRITICAL_ACCESS,
    }
    return lookup[value]

def str_to_rating(value):
    return None if value == 'Not Available' else int(value)

class HospitalType(Enum):
    CRITICAL_ACCESS = 'CRITICAL_ACCESS'
    ACUTE_CARE = 'ACUTE_CARE'
    CHILDRENS = 'CHILDRENS'

class ComparisonEnum(Enum):
    BELOW = 'BELOW'
    SAME = 'SAME'
    ABOVE = 'ABOVE'
    NA = 'NA'

enum_properties = set([
    'safetyOfCareComparison',
    'readmissionComparison',
    'patientExperienceComparison',
    'effectivenessComparison',
    'timelinessComparison',
    'medicalImaginingComparison',
])

boolean_properties = set([
    'emergencyServices',
    'meetsMeaningfulUse',
])

if __name__ == '__main__':
    filename = sys.argv[1]
    count = 0
    all_data = []
    with open(filename, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(row_reader)
        for data_row in row_reader:
            data = {}
            for i in range(0, len(data_row)):
                old_name = columns[i]
                new_name = name_lookup[old_name]
                if new_name in enum_properties:
                    data[new_name] = str_to_comparison_enum(data_row[i])
                elif new_name in boolean_properties:
                    data[new_name] = str_to_boolean(data_row[i])
                elif new_name == 'hospitalType':
                    data[new_name] = str_to_hospital_type(data_row[i])
                elif new_name == 'hospitalOwnership':
                    data[new_name] = str_to_ownership_enum(data_row[i])
                elif new_name == 'overallRating':
                    data[new_name] = str_to_rating(data_row[i])
                else:
                    data[new_name] = data_row[i]
            all_data.append(data)
            # print(data['hospitalType'])

    for column_name in [
            'hospitalType', 
            'hospitalOwnership', 
            'emergencyServices', 
            'meetsMeaningfulUse', 
            'overallRating', 
            'mortalityComparison',
            'safetyOfCareComparison',
            'readmissionComparison',
            'patientExperienceComparison',
            'effectivenessComparison',
            'timelinessComparison',
            'medicalImaginingComparison',
            ]:
        set_of_values = set([data[column_name] for data in all_data])
        print('column_name: %s values: %s' % (column_name, str(set_of_values)))

