class Solution:
    def moveZeroes(self, nums: list) -> None:
        k=nums.sort(key="0".__eq__)
        return k
p=Solution()
print(p.moveZeroes(nums = [0,1,0,3,12]))