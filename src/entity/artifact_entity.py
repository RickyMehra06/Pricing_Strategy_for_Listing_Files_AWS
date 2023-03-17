from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    raw_data_file_path:str
    master_data_file_path:str
    shipping_charge_file_path:str

@dataclass
class DataValidationArtifact:
    data_validation_artifact:str


@dataclass
class DataTransformationArtifact:
    invalid_data_path:str
    data_without_asin_path:str
    transformed_data_path:str

@dataclass
class TargetPriceGeneratorArtifact:
    target_sp_price_data_path:str


@dataclass
class FilesGenerationArtifact:
    usd_fillz_file_path:str
    usd_auto_file_path:str

    cad_fillz_file_path:str
    cad_auto_file_path:str
    
    aus_fillz_file_path:str
    aus_auto_file_path:str


