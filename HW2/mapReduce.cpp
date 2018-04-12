#include <iostream>       
#include <thread>         
#include <utility>			// std::pair
#include <vector>

template <typename Func_Map, typename Func_Reduce, typename mapReduceResult, typename mapReduceInput, typename mapResults, typename reduceResults> 
void mapReduce(Func_Map, Func_Reduce, mapReduceResult, mapReduceInput, mapResults, reduceResults, reduceResults, const int);

template <typename Func, typename mapResult, typename mapInput> 
void map(Func, mapResult, mapInput, const int);

template <typename Func, typename reduceResult, typename reduceInput> 
void reduce(Func, reduceResult, reduceInput, const int);

void mapper_int_count(std::vector<std::pair<int,int>> *, std::vector<int> *);
void reducer_int_count(std::vector<std::pair<int,int>> *, std::vector<std::vector<std::pair<int,int>>> *);

int map_branch_factor(const int, int = 4);
int reduce_branch_factor(const int, int = 1);


int main() 
{
	const int numCPU = 4;											// set number of computation cores
	
	const int input_size = 2 << 5; 
	std::vector<int> input;
	for(int i = 0; i < input_size; i++) input.push_back(i%10); 		// generate input
	
	std::vector<std::pair<int,int>> result; 
	std::vector<std::vector<std::pair<int,int>>> map_results;
	std::vector<std::vector<std::pair<int,int>>> reduce_results_tmp;
	std::vector<std::vector<std::pair<int,int>>> reduce_results;
	
	mapReduce(&mapper_int_count, &reducer_int_count, &result, &input, &map_results, &reduce_results_tmp, &reduce_results, numCPU);

	return 0;
}

template <typename Func_Map, typename Func_Reduce, typename mapReduceResult, typename mapReduceInput, typename mapResult, typename reduceResult> 
void mapReduce(Func_Map mapper, Func_Reduce reducer, mapReduceResult result, mapReduceInput input, mapResult map_results, reduceResult reduce_results_tmp, reduceResult reduce_results, const int numCPU){
	map(mapper, map_results, input, numCPU);
	
	/*for(auto i = (*map_results).begin(); i != (*map_results).end(); i++){
		for(auto j = (*i).begin(); j != (*i).end(); j++){
			std::cout << j->first << ": " << j->second << std::endl;
		}
		std::cout << std::endl;
	}*/
	
	reduce(reducer, reduce_results_tmp, map_results, numCPU);
	
	/*std::cout << "Reducer" << std::endl;
	
	for(auto i = (*reduce_results_tmp).begin(); i != (*reduce_results_tmp).end(); i++){
		for(auto j = (*i).begin(); j != (*i).end(); j++){
			std::cout << j->first << ": " << j->second << std::endl;
		}
		std::cout << std::endl;
	}
	*/
	
	reduce(reducer, reduce_results, reduce_results_tmp, 1);
	
	std::cout << "Reducer final" << std::endl;
	for(auto i = (*reduce_results).begin(); i != (*reduce_results).end(); i++){
		for(auto j = (*i).begin(); j != (*i).end(); j++){
			std::cout << j->first << ": " << j->second << std::endl;
		}
		std::cout << std::endl;
	}
	
	
}

template <typename Func, typename mapResult, typename mapInput>
void map(Func mapper, mapResult results, mapInput input, const int numCPU){
	std::thread th[numCPU];
	
	int input_size = (*input).size();
	int parts = map_branch_factor(numCPU);
	int part_size = input_size/parts;
	std::vector<std::vector<int>> input_partition;
	
	for(int i = 0; i < parts; i++){
		input_partition.push_back(std::vector<int>( (*input).begin()+(i*part_size), (*input).begin()+((i+1)*part_size)) );
		std::vector<std::pair<int,int>> entry;
		(*results).push_back(entry);
	}
	
	auto iter_input = input_partition.begin();
	auto iter_results = (*results).begin();
	
	for(int i = 0; iter_input != input_partition.end(); iter_input++, iter_results++, i++){
		if(i >= numCPU) th[i%numCPU].join();
		th[i%numCPU] = std::thread(mapper, &(*iter_results), &(*iter_input));
	}
	
	for(int i = 0; i < numCPU; i++) th[i].join();
	
}

template <typename Func, typename reduceResult, typename reduceInput>
void reduce(Func reducer, reduceResult result, reduceInput input, const int numCPU){
	std::thread th[numCPU];
	
	int maps_num = (*input).size(); 
	int part_size = maps_num/numCPU; 
	std::vector<std::vector<std::vector<std::pair<int,int>>>> input_partition;
	
	for(int i = 0; i < numCPU; i++){
		std::vector< std::vector< std::pair<int,int> > >::const_iterator first = (*input).begin() + i*part_size;
		std::vector< std::vector< std::pair<int,int> > >::const_iterator last = (*input).begin() + (i+1)*part_size;
		std::vector< std::vector< std::pair<int,int> > > newVec(first, last);
		input_partition.push_back( newVec );
		std::vector<std::pair<int,int>> entry;
		(*result).push_back(entry);
	}
	
	auto iter_input = input_partition.begin();
	auto iter_result = (*result).begin();
	
	for(int i = 0; iter_input != input_partition.end(); iter_input++, iter_result++, i++) th[i] = std::thread(reducer, &(*iter_result), &(*iter_input));
	for(int i = 0; i < numCPU; i++) th[i].join();
}

void mapper_int_count(std::vector<std::pair<int,int>> * result, std::vector<int> * input){
	auto iter_input = (*input).begin();
	auto iter_result = (*result).begin();
	
	for(; iter_input != (*input).end(); iter_input++){
		for(iter_result = (*result).begin(); iter_result != (*result).end(); iter_result++){
			if (*iter_input == iter_result->first) {
				(iter_result->second)++;
				break;
			}
		}
		if (iter_result == (*result).end()) {
			(*result).push_back(std::make_pair(*iter_input, 1));		
		}
	}
	
}

void reducer_int_count(std::vector<std::pair<int,int>> * result, std::vector<std::vector<std::pair<int,int>>> * inputs){
	auto iter_inputs = (*inputs).begin();
	auto iter_input = (*iter_inputs).begin();
	auto iter_result = (*result).begin();
	
	for(; iter_inputs != (*inputs).end(); iter_inputs++){
		for(iter_input = (*iter_inputs).begin(); iter_input != (*iter_inputs).end(); iter_input++){
			for(iter_result = (*result).begin(); iter_result != (*result).end(); iter_result++){
				if(iter_input->first == iter_result->first){
					(iter_result->second) += iter_input->second;
					break;
				}
			}
			if (iter_result == (*result).end()) {
				(*result).push_back(std::make_pair(iter_input->first, iter_input->second));		
			}
		}
	}
	
}

int map_branch_factor(const int numCPU, int factor){
	return(numCPU*factor);	
}

int reduce_branch_factor(const int numCPU, int factor){
	return(numCPU*factor);
}

