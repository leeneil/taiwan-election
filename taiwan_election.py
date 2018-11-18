import numpy as np
import pandas as pd
import csv
import json
import os

class TaiwanElection(object):
    def __init__(self, election):
        self.dataPath0 = "database"
        self.dataPath1 = election[0]
        self.dataPath2 = election[1]
        self.region_data = None
        self.vote_data = None
        self.candidate_data = None
        self.party_data = None

    def build(self):
        self.build_region()
        self.build_vote()
        self.build_candidate()
        self.build_party()

    def build_region(self):
        dataFilename = "elbase.csv"
        dataPath = os.path.join(self.dataPath0, self.dataPath1, self.dataPath2, dataFilename)
        self.region_data = pd.read_csv(dataPath, escapechar="'",
                                    names=["province", "county", "district",
                                            "city", "li", "name"])

    def build_vote(self):
        dataFilename = "elctks.csv"
        dataPath = os.path.join(self.dataPath0, self.dataPath1, self.dataPath2, dataFilename)
        self.vote_data = pd.read_csv(dataPath, escapechar="'", index_col=False,
                                    names=["province", "county", "district",
                                            "city", "li", "site_no",
                                            "candidate_no", "counts",
                                            "percent", "elected?"])

    def build_candidate(self):
        dataFilename = "elcand.csv"
        dataPath = os.path.join(self.dataPath0, self.dataPath1, self.dataPath2, dataFilename)
        self.candidate_data = pd.read_csv(dataPath, escapechar="'", index_col=False,
                                    names=["province", "county", "district",
                                            "city", "li", "candidate_no",
                                            "candidate_name", "party_no",
                                            "sex", "dob", "age", "pob",
                                            "education", "incumbent", "winner"])

    def build_party(self):
        dataFilename = "elpaty.csv"
        dataPath = os.path.join(self.dataPath0, self.dataPath1, self.dataPath2, dataFilename)
        self.party_data = pd.read_csv(dataPath, names=["party_no", "party_name"],
                                        )

    def search(self, keyword):
        return self.region_data[ self.region_data.li_name == keyword ]

    def list_city_vote(self, city):
        city = city.replace("台","臺")
        city_item= self.region_data[ self.region_data.li_name == city ]
        candidate_items = self.candidate_data.query("province == {}".format(city_item.province.iloc[0]))[["candidate_no", "candidate_name"]]
        print(candidate_items)
        region_df = self.region_data.query("province == {}".format(city_item.province.iloc[0]))
        vote_df = self.vote_data.query("province == {} and city == 0 and site_no == 0".format(city_item.province.iloc[0]))
        df = vote_df.merge(city_item[["province", "li_name"]], left_on="province", right_on="province")
        # df = df.merge(region_df[["li", "li_name"]], left_on="li", right_on="li")
        # df = df.merge( df, candidate_items, left_on="candidate_no", right_on="candidate_no")
        return df

    def summary(self, city, district=None, li=None):
        city = city.replace("台","臺")
        city_item = self.region_data.query( "name == '{}'".format(city) )
        query_string = "site_no == 0 and province == {}".format(city_item.province.iloc[0])
        if district != None:
            dist_item = self.region_data.query( "province == {} and name == '{}'".format(city_item.province.iloc[0], district) )
            query_string += " and city == {}".format(dist_item.city.iloc[0])
        else:
            query_string += " and city == 0"
        if li != None:
            li_item = self.region_data.query( "province == {} and city == {} and name == '{}'".format(city_item.province.iloc[0], dist_item.city.iloc[0], li) )
            query_string += " and li == {}".format(li_item.li.iloc[0])
        else:
            query_string += " and li == 0"

        candidate_items = self.candidate_data.query("province == {}".format(city_item.province.iloc[0]))

        vote_df = self.vote_data.query(query_string)
        df = vote_df.merge(city_item[["province", "name"]], left_on="province", right_on="province")
        if district != None:
            df = df.merge(dist_item[["city", "name"]], left_on="city", right_on="city")
            if li != None:
                df = df.merge(li_item[["li", "name"]], left_on="li", right_on="li")
        # df = df.merge( df, candidate_items[["candidate_no", "candidate_name"]], left_on="candidate_no", right_on="candidate_no")
        return df # candidate_items[["candidate_no", "candidate_name"]]

    def stats_by_li(self, city, district=None):
        city = city.replace("台","臺")
        city_item = self.region_data.query( "name == '{}'".format(city) )
        # print(city_item)
        query_string = "site_no == 0 and li!=0 and province == {}".format(city_item.province.iloc[0])
        if district != None:
            dist_item = self.region_data.query( "province == {} and name == '{}'".format(city_item.province.iloc[0], district) )
            query_string += " and city == {}".format(dist_item.city.iloc[0])
            dist_names = self.region_data.query( "province == {} and city == {} and (li == 0 or li == '0000')".format(city_item.province.iloc[0], dist_item.city.iloc[0]) )
            li_names = self.region_data.query( "province == {} and city == {}".format(city_item.province.iloc[0], dist_item.city.iloc[0]) )
        else:
            query_string += " and city != 0"
            dist_names = self.region_data.query( "province == {} and city != 0 and (li == 0 or li == '0000')".format(city_item.province.iloc[0]) )
            li_names = self.region_data.query( "province == {} and city != 0".format(city_item.province.iloc[0]) )


        # candidate_items = self.candidate_data.query("province == {}".format(city_item.province.iloc[0]))

        # print(dist_names)
        # print(li_names)
        vote_df = self.vote_data.query(query_string)
        df = vote_df.merge(city_item[["province", "name"]], left_on="province", right_on="province")
        # if district != None:
        #     df = df.merge(dist_item[["city", "name"]], left_on="city", right_on="city")
        df = df.merge(dist_names[["city", "name"]], left_on="city", right_on="city")
        df = df.merge(li_names[["city", "li", "name"]], left_on=["city", "li"], right_on=["city", "li"])
        # df = df.merge( df, candidate_items[["candidate_no", "candidate_name"]], left_on="candidate_no", right_on="candidate_no")
        return df # candidate_items[["candidate_no", "candidate_name"]]

    def list_li(self, city, district=None):
        city = city.replace("台","臺")
        city_item = self.region_data[ self.region_data.name == city ]
        query_string = "province == {} and li != 0".format(city_item.province.iloc[0])
        if district != None:
            dist_item = self.region_data.query( "province == {} and name == '{}'".format(city_item.province.iloc[0], district) )
            query_string += " and city == {}".format(dist_item.city.iloc[0])
        df = self.region_data.query(query_string)#["city, ""name"]
        dist_names = self.region_data.query( "province == {} and li == 0".format(city_item.province.iloc[0]) )[["city", "name"]]
        df = df.merge( dist_names, left_on="city", right_on="city")
        return df

    def list_candidates(self, city):
        city = city.replace("台","臺")
        city_item = self.region_data[ self.region_data.name == city ]
        df = self.candidate_data.query("province == {}".format(city_item.province.iloc[0]))#["city, ""name"]
        parties = self.party_data[["party_no", "party_name"]]
        df = df.merge( parties, left_on="party_no", right_on="party_no")
        return df
