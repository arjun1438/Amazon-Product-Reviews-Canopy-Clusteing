ant build
step1()
{
   echo "you chose choice 1"
   hadoop fs -rm -R hw1/output/step1
   hadoop fs -rm -R hw1/input/step1
   hadoop fs -put 100k_lines.txt hw1/input/step1
   yarn jar build/lib/KmeansClustering.jar kmeans.amazon.clustering.AmazonReviewAnalysisDataAggregate hw1/input/step1 hw1/output/step1 
   hadoop fs -getmerge hw1/output/step1/part* step1_output.txt
}

step2()
{
   echo "you chose choice 2"
   hadoop fs -rm -R hw1/output/step2
   hadoop fs -rm -R hw1/input/step2
   hadoop fs -put step1_output.txt hw1/input/step2
   yarn jar build/lib/KmeansClustering.jar kmeans.amazon.clustering.AmazonReviewCanopyCenterFinder hw1/input/step2 hw1/output/step2
}

step3()
{
   echo "you chose choice 3"
   hadoop fs -rm -R hw1/output/step3
   hadoop fs -rm -R hw1/input/step3
   hadoop fs -put step1_output.txt hw1/input/step3
   yarn jar build/lib/KmeansClustering.jar kmeans.amazon.clustering.AssignAmazonProductsToCanopies hw1/input/step3 hw1/output/step3
}

runAll()
{
    step1
    step2
    step3
}   


PS3='Please enter your choice: '
options=("Step 1" "Step 2" "Step 3" "runAll" "Quit")
select opt in "${options[@]}"
do
    case $opt in
        "Step 1")
            step1
            ;;
        "Step 2")
            step2
            ;;
        "Step 3")
            step3
            ;;
        "runAll")
            runAll
            ;;
        "Quit")
            break
            ;;
        *) echo invalid option;;
    esac
done
