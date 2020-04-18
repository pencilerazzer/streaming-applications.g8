import json
import zlib
import base64
import sys
import os
import copy
import os.path as op

SCHEMA_FOLDER = op.join(op.dirname(
    op.dirname(op.abspath(__file__))), "streaming_pipelines/src/main/avro/")
BLUEPRINT_FOLDER = op.join(op.dirname(op.dirname(
    op.abspath(__file__))), "streaming_pipelines/src/main/blueprint/")
RESOURCES_FOLDER = op.join(op.dirname(op.dirname(
    op.abspath(__file__))), "streaming_pipelines/src/main/resources/")
JSON_FOLDER = op.dirname(op.dirname(op.abspath(__file__)))
CODEGEN_PATH = op.join(op.dirname(op.dirname(
    op.abspath(__file__))), "codegen/src/main/resources/streamlets_info.json")


def read_config(path):
    with open(path, "r") as f:
        config = json.load(f)
    return config


def find(lst, value):
    print("connections>>>", lst)
    for i, dic in enumerate(lst):
        for key in dic.keys():
            if dic[key] == value:
                return key
    return 0

def find2(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    return 0

def encode_config(config):
    data = json.dumps(config)
    data = base64.b64encode(zlib.compress(data.encode("utf8"))).decode('utf8')
    print(data)
    print(str(data))
    return data


def write_blueprint(blueprint, path):
    json_string = json.dumps(blueprint)

    json_string = json_string.replace('"', '')

    json_string = json_string.replace(':', '=')

    json_string = json_string.replace(",", '\n')

    json_string = json_string[1:-1]

    with open(path, 'w') as f:
        f.write(json_string)


def write_config_params_hocon(blueprint, path):
    write_config(blueprint, "dummy.json")
    os.system("cat dummy.json | pyhocon -f hocon > {}".format(path))


def write_config(content, path):
    with open(path, "w") as f:
        json.dump(content, f, indent=4)


def fix_blueprint(blueprint1):
    blueprint = copy.deepcopy(blueprint1)
    blueprint["blueprint"] = {}
    blueprint["blueprint"]["streamlets"] = {}
    blueprint["blueprint"]["connections"] = {}
    for streamlet in blueprint1["blueprint"]["streamlets"].keys():
        val = blueprint1["blueprint"]["streamlets"][streamlet]
        newKey = streamlet.replace("_", "-")
        print("newkey======>  ", newKey)
        blueprint["blueprint"]["streamlets"][newKey] = val
    for streamlet, connections in blueprint1["blueprint"]["connections"].items():
        value = [connection.replace("_", "-") for connection in connections]
        print("value=======>   ", value)
        newKey = streamlet.replace("_", "-")
        print("newkey======>  ", newKey)
        blueprint["blueprint"]["connections"][newKey] = value
    print("newblueprint============>   ", blueprint)
    return blueprint


def get_schema(streamlet_name, config):
    fields = []
    field_names = []
    if "databases" in streamlet_name:
        for table in config["steps_param"][streamlet_name]["tables"]:
            for field in table["operations"]["selectcolumns"]:
                if field["columnName"] in field_names:
                    continue
                else:
                    schema = {}
                    schema["name"] = field["columnName"]
                    field_names.append(field["columnName"])
                    schema["default"] = None
                    if field["type"] == "null":
                        schema["type"] = [field["type"]]
                    else:
                        schema["type"] = ["null", field["type"]]
                    fields.append(schema)
    elif "filesystem" in streamlet_name:
        if "s3" in streamlet_name or "gcs" in streamlet_name:
            for bucket in config["steps_param"][streamlet_name]["buckets"]:
                for field in bucket["operations"]["selectcolumns"]:
                    if field["columnName"] in field_names:
                        continue
                    else:
                        schema = {}
                        schema["name"] = field["columnName"]
                        field_names.append(field["columnName"])
                        schema["default"] = None
                        if field["type"] == "null":
                            schema["type"] = [field["type"]]
                        else:
                            schema["type"] = ["null", field["type"]]
                        fields.append(schema)
        else:
            for field in config["steps_param"][streamlet_name]["operations"][
                "selectcolumns"
            ]:
                if field["columnName"] in field_names:
                        continue
                else:
                    schema = {}
                    schema["name"] = field["columnName"]
                    field_names.append(field["columnName"])
                    schema["default"] = None
                    if field["type"] == "null":
                        schema["type"] = [field["type"]]
                    else:
                        schema["type"] = ["null", field["type"]]
                    fields.append(schema)
    elif "streamstreamjoin" in streamlet_name:
        for field in config["steps_param"][streamlet_name]["output_schema"]:
                if field["columnName"] in field_names:
                        continue
                else:
                    schema = {}
                    schema["name"] = field["columnName"]
                    field_names.append(field["columnName"])
                    schema["default"] = None
                    if field["type"] == "null":
                        schema["type"] = [field["type"]]
                    else:
                        schema["type"] = ["null", field["type"]]
                    fields.append(schema)
    elif "kafka" in streamlet_name:
        for field in config["steps_param"][streamlet_name]["operations"]["selectcolumns"]:
            if field["columnName"] in field_names:
                continue
            else:
                schema = {}
                schema["name"] = field["columnName"]
                field_names.append(field["columnName"])
                schema["default"] = None
                if field["type"] == "null":
                    schema["type"] = [field["type"]]
                else:
                    schema["type"] = ["null", field["type"]]
                fields.append(schema)
    return fields


def write_schema(fields, record_name):
    record_file_name = record_name + ".avsc"
    record_file_destination = op.join(SCHEMA_FOLDER, record_file_name)
    record = {"namespace": "applications", "name": record_name, "type": "record"}
    record["fields"] = fields
    write_config(record, record_file_destination)


def write_config_params(config_encripted, streamlets, kafkaaddress, kafkaport, schema_registry_url):
    VOLUME_MOUNT_NAME = "/tmp/cloudflow/"
    CONFIG_FILENAME_NAME = "master_config.json"
    POLLING_INTERVAL_VALUE = 5
    MOUNT_PARAM_NAME = "configuration"
    CONFIG_FILENAME_PARAM_NAME = "filter-filename"
    POLLING_INTERVAL_PARAM_NAME = "filter-pollinginterval"
    CONFIG_ROOT_KEY_PARAM_NAME = "config-key-root"
    CONFIG_PARAM_NAME = "generic-ingress-config"
    SCHEMA_REGISTRY_URL_NAME = "schema-registry-url"

    CONFIG_PARAMS = {}
    CONFIG_PARAMS["kafka-params"] = {}
    CONFIG_PARAMS["kafka-params"]["address"] = kafkaaddress
    CONFIG_PARAMS["kafka-params"]["port"] = int(kafkaport)

    for streamlet in streamlets:
        streamlet = streamlet.replace("_", "-")
        CONFIG_PARAMS[streamlet] = {}
        CONFIG_PARAMS[streamlet][MOUNT_PARAM_NAME] = VOLUME_MOUNT_NAME
        CONFIG_PARAMS[streamlet][CONFIG_FILENAME_PARAM_NAME] = CONFIG_FILENAME_NAME
        CONFIG_PARAMS[streamlet][POLLING_INTERVAL_PARAM_NAME] = POLLING_INTERVAL_VALUE
        CONFIG_PARAMS[streamlet][CONFIG_ROOT_KEY_PARAM_NAME] = streamlet
        CONFIG_PARAMS[streamlet][CONFIG_PARAM_NAME] = config_encripted
        CONFIG_PARAMS[streamlet][SCHEMA_REGISTRY_URL_NAME] = schema_registry_url
    write_config_params_hocon(
        CONFIG_PARAMS, op.join(RESOURCES_FOLDER, "local.conf"))


def get_streamlet_mapping_config(name, streamlet_mapping, num_split=4):
    if num_split == 1:
        final_value = streamlet_mapping[name]
        return final_value
    else:
        result = name.split("_", 1)
        key = result[0]
        value = result[1]
        return get_streamlet_mapping_config(value, streamlet_mapping[key], num_split - 1)


def get_streamlet_class(name, streamlet_mapping, num_split=4):
    if num_split == 1:
        if streamlet_mapping[name]["makeCode"]:
            final_value = [streamlet_mapping[name]["absClass"] +
                           name.split("_")[-1], streamlet_mapping[name]["absClass"]]
        else:
            final_value = [streamlet_mapping[name]["absClass"]]
        return final_value
    else:
        result = name.split("_", 1)
        key = result[0]
        value = result[1]
        return get_streamlet_class(value, streamlet_mapping[key], num_split - 1)


# def get_blueprint(config, streamlet_mapping, config_encripted):
#     blueprint = {}
#     blueprint["blueprint"] = {}
#     blueprint["blueprint"]["connections"] = {}
#     blueprint["blueprint"]["streamlets"] = {}
#     streamlet_info = {}
#     potential_ingress = []
#     potential_flows = []
#     potential_egress = []
#     left = []
#     right = []
#     for _, value in config["steps"].items():
#         streamlet_name = value[0]
#         left.append(streamlet_name)
#         right.extend(value[1])
#     from collections import Counter
#     l = Counter(left)
#     r = Counter(right)
#     for key, value in l.items():
#         if value == 1:
#             l[key] = 0
#     for key, value in r.items():
#         if value == 1:
#             r[key] = 0
#     for _, value in config["steps"].items():
#         streamlet_name = value[0]
#         newValue = []
#         if l[streamlet_name] == 0:
#             newKey = streamlet_name + ".out"
#         else:
#             newKey = streamlet_name + ".out{}".format(l[streamlet_name])
#             l[streamlet_name] = l[streamlet_name] - 1
#         for val in value[1]:
#             if r[val] == 0:
#                 newValue.append(val + ".in")
#             else:
#                 newValue.append(val + ".in{}".format(r[val]))
#                 r[val] = r[val] - 1
#         blueprint["blueprint"]["connections"][newKey] = newValue
#         potential_ingress.append(streamlet_name)
#         potential_flows.extend(value[1])
#     print(blueprint)
#     potential_ingress_copy = potential_ingress.copy()
#     for idx, streamlet in enumerate(potential_ingress):
#         if streamlet in potential_flows:
#             potential_ingress.pop(idx)
#     for idx, streamlet in enumerate(potential_flows):
#         if streamlet not in potential_ingress_copy:
#             potential_egress.append(potential_flows.pop(idx))
#     for streamlet in potential_ingress:

#         blueprint["blueprint"]["streamlets"][streamlet] = get_streamlet_class(
#             streamlet, streamlet_mapping, streamlet_info
#         )
#     for streamlet in potential_flows:
#         blueprint["blueprint"]["streamlets"][streamlet] = get_streamlet_class(
#             streamlet, streamlet_mapping, streamlet_info
#         )
#     for streamlet in potential_egress:
#         blueprint["blueprint"]["streamlets"][streamlet] = get_streamlet_class(
#             streamlet, streamlet_mapping, streamlet_info
#         )
#     for streamlet_name in potential_ingress:
#         fields, field_names = get_schema(
#             streamlet_name, config, blueprint["blueprint"]["streamlets"][streamlet_name], fields, field_names)
#         portType =

#     write_schema(fields)
#     blueprint2 = fix_blueprint(blueprint)
#     write_blueprint(blueprint2, op.join(BLUEPRINT_FOLDER, "blueprint.conf"))
#     write_config_params(config_encripted, potential_ingress +
#                         potential_flows + potential_egress)
#     return blueprint, potential_ingress, potential_flows, potential_egress

#     def update_streamlet_info(streamlet_class, streamlet_info_overall, portClass):
#         if find(streamlet_info_overall["streamlets"], "name", streamlet_class[0]):
#             idx = find(
#                 streamlet_info_overall["streamlets"], "name", streamlet_class[0])
#             streamlet_info = streamlet_info_overall["streamlets"][idx]
#             streamlet_info["ports"].append(portClass)
#             streamlet_info_overall[idx] = streamlet_info
#         else:
#             streamlet_info = {}
#             streamlet_info["name"] = streamlet_class
#             streamlet_info["absClass"] = streamlet_class[1]
#             streamlet_info["ports"] = []
#             streamlet_info["ports"].append(portClass)
#             streamlet_info_overall.append(streamlet_info)

def get_port_type(full_port_name):
    if 'in' in full_port_name.split(".")[-1]:
        return "inlets"
    else:
        return "outlets"

def get_portclass(full_port_name, streamlet_mapping, connections, config, portclass_mapping):
    if full_port_name in portclass_mapping.keys():
        return portclass_mapping[full_port_name]
    else:
        streamlet = full_port_name.split(".")[0]
        port_type = get_port_type(full_port_name)
        port_name = full_port_name.split(".")[-1]
        streamlet_mapping_config = get_streamlet_mapping_config(
            streamlet, streamlet_mapping)
        idx = find2(streamlet_mapping_config[port_type], "name", port_name)
        if streamlet_mapping_config[port_type][idx]["schema"] == "generate":
            streamelet_class = get_streamlet_class(
                streamlet, streamlet_mapping)
            fields = get_schema(streamlet, config)
            portclass = streamelet_class[0].split(".")[-1][:-1] + "Record" + port_name
            write_schema(fields, portclass)
            portclass_mapping[full_port_name] = portclass
            return portclass
        elif streamlet_mapping_config[port_type][idx]["schema"] == "connection":
            new_streamlet_full_port_name = find(connections, full_port_name)
            print(new_streamlet_full_port_name)
            return get_portclass(new_streamlet_full_port_name, streamlet_mapping, connections, config, portclass_mapping)
        elif streamlet_mapping_config[port_type][idx]["schema"] == "FlowingFrame":
            return "FlowingFrame"
        elif streamlet_mapping_config[port_type][idx]["schema"] == "SessionUpdate":
            return "SessionUpdate"
        else:
            print( streamlet, port_type, streamlet_mapping_config)
            new_streamlet_full_port_name = streamlet + "." + streamlet_mapping_config[port_type][idx]["schema"]
            return get_portclass(new_streamlet_full_port_name, streamlet_mapping, connections, config, portclass_mapping)

def get_blueprint2(config, streamlet_mapping , kafkaaddress, kafkaport, schema_registry_url):
    project_name = "pipelines_testing" 
    blueprint = {}
    blueprint["blueprint"] ={}
    blueprint["blueprint"]["streamlets"] = {}
    blueprint["blueprint"]["connections"] = {}
    streamlet_info_overall = {"streamlets": []}
    connections = []
    portclass_mapping = {}
    for _, value in config["steps"].items():
        cons = []
        streamlet_name = value[0]
        if "staticdataframe" in streamlet_name:
            for con in value[1]:
                if "static"  in con:
                    config["steps_param"][con.split(".")[0]].update(config["steps_param"][streamlet_name.split(".")[0]])
        else:
            blueprint["blueprint"]["connections"][streamlet_name] = value[1]
            connections.extend([{streamlet_name: con}  for con in value[1]])
    for streamlet, _ in config["steps_param"].items():
        if "staticdataframe" in streamlet:
            continue
        streamlet_mapping_config = get_streamlet_mapping_config(streamlet, streamlet_mapping)
        streamlet_class = get_streamlet_class(streamlet, streamlet_mapping)
        print("streamlet_mapping_config", streamlet_mapping_config)
        if streamlet_mapping_config["makeCode"]:
            blueprint["blueprint"]["streamlets"][streamlet] = "applications" + "." + streamlet_class[0].split(".")[-1]
            streamlet_info = {"name":streamlet_class[0].split(".")[-1], "absClass": streamlet_class[1], "ports": []}
            for i in streamlet_mapping_config["inlets"]:
                inlet = i["name"]
                full_port_name = streamlet + "." + inlet
                portclass = get_portclass(full_port_name, streamlet_mapping,
                                         connections,config, portclass_mapping)
                streamlet_info["ports"].append(portclass)
            for j in streamlet_mapping_config["outlets"]:
                outlet = j["name"]
                full_port_name = streamlet + "." + outlet
                portclass = get_portclass(full_port_name, streamlet_mapping,
                                         connections,config, portclass_mapping)
                streamlet_info["ports"].append(portclass)
            streamlet_info_overall["streamlets"].append(streamlet_info)
        else:
            blueprint["blueprint"]["streamlets"][streamlet] = streamlet_class[0]
    blueprint2 = fix_blueprint(blueprint)
    write_blueprint(blueprint2, op.join(BLUEPRINT_FOLDER, "blueprint.conf"))
    config_encripted = config_encripted = encode_config(config)
    write_config_params(config_encripted, blueprint["blueprint"]["streamlets"].keys(), kafkaaddress, kafkaport, schema_registry_url)
    write_config(streamlet_info_overall, CODEGEN_PATH)
    print(streamlet_info_overall)
    print(blueprint)
    return blueprint


def main(config_path, kafkaaddress, kafkaport, schema_registry_url, streamlet_mapping_path="./streamlet_mapping.json"):
    config = read_config(config_path)
    # print("config_encripted===================>", config_encripted)
    streamlet_mapping = read_config(streamlet_mapping_path)
    # config_encripted = config_encripted.replace('\n', "")
    return get_blueprint2(config, streamlet_mapping, kafkaaddress, kafkaport, schema_registry_url)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
