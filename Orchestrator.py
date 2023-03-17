import config
import diskcache as dc
import importlib
from timeit import default_timer as timer
from datetime import timedelta

from sources.final_steps import final_steps
from sources.startup import startup

class Orchestrator():
    """ This class runs all the jobs """
    
    caches=dc.Cache()
   

    
    jobs=[]
    totalTimes={}

    def start(self):
        """ This method defines the class dynamically and imports the modules """

        startup(self.caches).run()

        
        for r in config.ordered_scripts:
            print("Control 1")
            """ Define the class dynamically"""
            module = importlib.import_module('sources.'+r)
            """ The getattr() method returns the value of the named attribute of an object """
            # Produce excepcion, quizas --> class_ = module.__name__ 
            class_ = getattr(module, r)
            instance = class_(self.caches)
            self.jobs.append(instance)

        self.runJobs()

        
        final_steps(self.caches).run()


    def runJobs(self):
        """ This method starts the inicial jobs """

        print("\n\n*** CONTROL INICIAL RUN JOBS ***")
        """ Run the job"""
        for job in self.jobs:
            print("\n\n*** JOB: ",job)            
            startTime=timer()
            job.run()
            
            self.totalTimes[type(job).__name__]=timer() -startTime
            print(type(job).__name__+' Total Time:'+ str(timedelta(seconds=self.totalTimes[type(job).__name__])))
        
        print("---------------  TOTAL TIMES  -----------------------------")
        for job in self.totalTimes:
            
            print(job+' Total Time: '+ str(timedelta(seconds=self.totalTimes[job])))