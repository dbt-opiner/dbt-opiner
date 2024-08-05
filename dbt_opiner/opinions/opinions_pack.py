from dbt_opiner.config_singleton import ConfigSingleton


class OpinionsPack:
    """
    Loads and holds all the opinions and the custom opinions.
    """

    def __init__(self):
        self._opinions = []
        config = ConfigSingleton().get_config()
        ignored_opinions = config.get("global").get("ignore_opinions", [])

        # Load default opinions
        from dbt_opiner.opinions import opinion_classes

        for opinion_class in opinion_classes:
            if opinion_class.__name__ not in ignored_opinions:
                self._opinions.append(opinion_class())

        # TODO Load custom opinions

    # Organize opinons by file and node type
    def get_opinions(self, file_type, node_type):
        """
        Get all the opinions that apply to a file and node type.
        """
        return [
            opinion
            for opinion in self._opinions
            if opinion.applies_to_file_type == file_type
            and opinion.applies_to_node_type == node_type
        ]
