# Return a dictionary with the numbers as keys and the number of times they appear in the list as values

def sort_and_count(nums):
  counts = {}               # Create a dictionary to store the counts.
  for i in nums:
    if i in counts:
      counts[i] += 1
    else:
      counts[i] = 1
  # Sort the dictionary by the values
  sorted_counts = sorted(counts.items(), key=lambda x: x[1])
  # Return the sorted dictionary
  return sorted_counts

nums = [1, 2, 4, 4, 5, 5, 5, 5, 7, 8, 9, 0, 0, 0, 0]
sorted_counts = sort_and_count(nums)
print(sorted_counts)