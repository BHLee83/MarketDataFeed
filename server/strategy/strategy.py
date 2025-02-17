from DiversifiedTrend.DiversifiedTrend import DiversifiedTrend
# from server.strategy.DiversifiedTrend.DiversifiedTrend import DiversifiedTrend


class Strategy:
    def __init__(self):
        self.strat_classes = {
            "DiversifiedTrend": DiversifiedTrend
        }
        self.strat_params = {
            "DiversifiedTrend": {
                "start_date": '20250102'
            }
        }

        for key, item in self.get_strategies():
            obj = item(self.get_params(key)['start_date'])
            obj.main()


    def run(self):
        for key, item in self.get_strategies():
            obj = item(self.get_params(key)['start_date'])
            if obj.isRT:
                obj.main()


    def get_params(self, strat_name):
        return self.strat_params[strat_name]


    def get_strategies(self):
        return self.strat_classes.items()
    

if __name__ == "__main__":
    Strategy()