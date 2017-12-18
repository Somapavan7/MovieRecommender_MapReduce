from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import numpy
from scipy import spatial

class movie_similarity(MRJob):   
    def configure_options(self):
        super(movie_similarity, self).configure_options()
        self.add_passthrough_option(
                '-m', '--movietitle', action="append", type='str', default=[], help='pass movie names to find similarities')
        self.add_passthrough_option(
		         '-l', '--limit', type ='float', default=0.4, help ='setting limitation for similaity')
        self.add_passthrough_option(
                '-p', '--ratingpairs', type='int', default=3, help=' pass number rating pairs between the given movie names') 
        
    def steps(self):
        return [
            MRStep(mapper=self.m_passingdata,
                reducer=self.r_joiningdata),
          MRStep(reducer=self.r_movieratingpairs),
          MRStep(reducer=self.r_appendpairs),
          MRStep(reducer=self.m_moviessimilarity)
    ]  
    def m_passingdata(self, _, line):
                fields = line.split(",")
                if (len(fields) == 3): # movie data 
                          yield fields[0], fields[1]
                else: # rating data
                          yield fields[1], (fields[0], fields[2])
                          
    def r_joiningdata(self, _, values):
                movie_list = list(values)
                movie_name = movie_list[0]
                values = movie_list[1:]
                for fields in values:
                        user_id = fields[0]
                        movierating = fields[1] 
                        yield user_id, (movie_name, movierating)     #key= user_id,value=(movie_title,rating) 
                        
    def r_movieratingpairs(self,user_id,movies):
         for pair1,pair2 in combinations(movies,2):
            movie_name1=pair1[0]
            movie_name2=pair2[0]
            movie_rating1=pair1[1]
            movie_rating2=pair2[1]
            yield (movie_name1,movie_name2),(movie_rating1,movie_rating2) #key=(title1,title2),value=(rating1,rating2)
            
    def r_appendpairs(self,movie_names,ratings):
        rating_pairs=[]
        for pairs in ratings:
            rating_pairs.append(pairs)
        yield movie_names,rating_pairs   #key=(title),value=(all ratings)
                          
    def m_moviessimilarity(self,movie_names,rating_pairs):
        movie_rating =list(rating_pairs)
        for ratings in movie_rating:
            l=len(ratings)
        rating1=[]
        rating2=[]
        for r in ratings:
            rating1.append(float(r[0]))
            rating2.append(float(r[1]))
        
        if(l>self.options.ratingpairs):
            for movie_name in self.options.movietitle:
                cor = numpy.corrcoef(rating1,rating2)[0,1]
                cos_cor = 1-spatial.distance.cosine(rating1,rating2)
                avg_cor = 0.5*(cor+cos_cor)
                if movie_names[0]== movie_name and cor > self.options.limit:
                    yield movie_names[0],(movie_names[1],cor,cos_cor,avg_cor,l)
                elif movie_names[1]==movie_name and cor > self.options.limit:
                    yield movie_names[1],(movie_names[0],cor,cos_cor,avg_cor,l)
     
if __name__ == '__main__':
            movie_similarity.run()
