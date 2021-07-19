"""
Author: Swapna Josmi Sam
Date: 19/07/2021
"""

# Import required libraries
import numpy as np
import random

def create_matrix(matrix_shape, points):
    # Create a nested list and stack the list to form a matrix
    matrix = [[[0]*points]*matrix_shape]*matrix_shape
    game_matrix = np.stack(matrix)
    
    # print(game_matrix.shape) ---> 8*8*2
    return game_matrix
    
    
def choose_random_coordinate_points(game_matrix, required_points):
    i = 0
    dim = game_matrix.shape[0]
    points_list = []
    
    while i < required_points:
        
        # Choose the coordinates given the shape of the matrix
        x = random.randint(0, dim-1)
        y = random.randint(0, dim-1)
        
        # add only the coordinate points that do not exists in the points list
        if (x,y) not in points_list:
            points_list.append((x,y))
            i+=1
            
    return points_list


def calculate_distance_matrix(game_matrix, points_list):
    
    # loop through each matrix row and column cell and calculate the distance
    # of that cell from the chosen points.
    for i in range(game_matrix.shape[0]):
        for j in range(game_matrix.shape[1]):
            
            distance_points = []
            for k in range(len(points_list)):
                
                distance = abs(points_list[k][0] - i) + abs(points_list[k][1] - j)
                distance_points.append(distance)
            game_matrix[i][j] = distance_points
    return game_matrix
    
    
def player_choose_point(distance_matrix, chances, points):
    i = 1
    while i <= chances:
        
        if i == chances:
            print("Game over! Please try again. \n")
        
        if len(points) > 0:
            # Request player to provide input.
            input_value = input(f"Please enter your points (attempt {i}): ")
            
            (result, index) = check_user_input(input_value, distance_matrix)
            
            if result == 0:
                print("Yeyyyy!!! you have found one point... \n")
                # remove the found points and recalculate the distance
                points.pop(index)

                # recalculate the new distance matrix with distance of only the left over points
                new_matrix = create_matrix(distance_matrix.shape[0], len(points))
                distance_matrix = calculate_distance_matrix(new_matrix, points)
                i+=1
            elif result == -1:
                print(f"Please select points between the range 0 and {distance_matrix.shape[0]-1}!!! \n")
                i+=1
            elif result == 1 or result == 2:
                
                print("You are HOT")
                i+=1
            elif result == 3 or result == 4:
                print("You are warm")
                i+=1
            elif result >= 5:
                print("You are cold")
                i+=1
        else:
            print("Congratulations!!! You have won!! \n")
            break
            
        
def check_user_input(input_value, distance_matrix):
    # split the input string to get the numbers and convert it to integers
    points = input_value.split(",")
    
    if((len(points) == 2) & \
        (int(points[0]) < distance_matrix.shape[0]) & \
        (int(points[1]) < distance_matrix.shape[0])):
        # get the distance of the selected coordinate points
        val = list(distance_matrix[int(points[0])][int(points[1])])
        
        # find the minimum distance among the points 
        min_val = min(val)
        min_index = val.index(min_val)
        
        return min_val, min_index
    else:
        return -1, -1
        
    
if __name__ == "__main__":
    
    print("Loading the game of Battleships... \n")
    
    matrix_shape = 8  # size of the board
    required_points = 2 #  number of points
    chances = 20  # user chances
    
    # create a matrix
    game_matrix = create_matrix(matrix_shape, required_points)
    # choose two random coordinate points
    points_list = choose_random_coordinate_points(game_matrix, required_points)
    print(points_list)
    
    # calculate distance of each matrix cell to the  selected points
    distance_matrix = calculate_distance_matrix(game_matrix, points_list)
    # print(distance_matrix)  
    
    print("Lets begin the game... \n")
    
    player_choose_point(distance_matrix, chances, points_list)
    print("\n")
    
    
    