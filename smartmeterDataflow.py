import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def parse_json(b):
    return json.loads(b.decode("utf-8"))

def to_bytes(d):
    return json.dumps(d).encode("utf-8")

def valid_record(d):
    p = d.get("pressure_kpa", None)
    t = d.get("temperature_c", None)
    if p is None or t is None:
        return False
    try:
        float(p)
        float(t)
        return True
    except:
        return False

def convert_units(d):
    p_kpa = float(d["pressure_kpa"])
    t_c = float(d["temperature_c"])
    d["pressure_psi"] = p_kpa / 6.895
    d["temperature_f"] = t_c * 1.8 + 32
    return d

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        save_main_session=True,
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input)
            | "ToDict" >> beam.Map(parse_json)
            | "FilterMissing" >> beam.Filter(valid_record)
            | "ConvertUnits" >> beam.Map(convert_units)
            | "ToBytes" >> beam.Map(to_bytes)
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=known_args.output)
        )

if __name__ == "__main__":
    run()
