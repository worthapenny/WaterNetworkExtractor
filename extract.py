import os
import winsound
#import pandas as pd

from simpledbf import Dbf5


def read_dbf(dbf_file_path):
    return Dbf5(dbf_file_path).to_dataframe()


def write_to_file(file_path, contents):
    with open(file_path, 'w') as f:
        f.write(contents)


def get_network(iwdb_path):
    if not os.path.exists(iwdb_path):
        raise FileNotFoundError('Given path "%s" is not valid.' % iwdb_path)

    # Read Node files
    file_path = os.path.join(iwdb_path, "NODE.DBF")
    if not os.path.exists(file_path):
        raise FileNotFoundError('Given path "%s" is not valid.' % file_path)

    nodes = read_dbf(file_path)
    if not nodes is None:
        nodes = nodes[["ID"]]

    # Read Node files
    file_path = os.path.join(iwdb_path, "LINK.DBF")
    if not os.path.exists(file_path):
        raise FileNotFoundError('Given path "%s" is not valid.' % file_path)

    links = read_dbf(file_path)
    if not links is None:
        links = links[["ID", 'FROM', 'TO']]

    # Rename the columns
    nodes.rename(inplace=True, columns={"ID": "id"})
    links.rename(inplace=True, columns={
        "ID": "id", "FROM": "source", "TO": "target"})

    return nodes, links


def export_to_json(nodes, links):
    network = {
        "nodes": nodes.to_dict(orient='records'),
        "links": links.to_dict(orient='records')
    }

    return str(network).replace("'", '"')


def export_to_csv(nodes, links, dir_path, base_file_name):
    nodes_csv = nodes.to_csv(index=False)
    links_csv = links.to_csv(index=False)

    write_to_file(os.path.join(
        dir_path, base_file_name + "_nodes.csv"), nodes_csv)

    write_to_file(os.path.join(
        dir_path, base_file_name + "_links.csv"), links_csv)

    return


iwdb_path = r'D:\Office\SCADAWatch\__UserFiles\PurpleTown_SW\__InfoWater Model\PURPLETOWN_SW.IWDB'
nodes, links = get_network(iwdb_path)


dir_name, model_name = os.path.split(iwdb_path)
model_name = model_name.split('.')[0]
json_file_path = os.path.join(dir_name, model_name + ".json")

# Export to JSON
network_json = export_to_json(nodes, links)
write_to_file(json_file_path, network_json)

# Export to CSV
export_to_csv(nodes, links, dir_name, model_name)

# Done
winsound.Beep(400, 300)
