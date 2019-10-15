package main

import(
	log "github.com/sirupsen/logrus"
	"fmt"
	"strings"
	// "bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	. "github.com/logrusorgru/aurora"
	// "github.com/rs/xid"
)
const sqsMaxMessages int64 = 5
const sqsPollWaitSeconds int64 = 1
var sess = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))
var sqsService = sqs.New(sess)
var s3Service = s3.New(sess)
var uploader = s3manager.NewUploader(sess)
var downloader = s3manager.NewDownloader(sess)
var bucket = "ta-bucket-josemarinas"

func main() {
	inputQueue, err := getQueueUrlByTag("Flow", "input")
	if err != nil {
		return
	}
	// outputQueue, err := getQueueUrlByTag("Flow", "output")
	if err != nil {
		return
	}
	fmt.Println(searchMessages(aws.String("Foo"), "paa"))
	inMsgChan := make(chan *sqs.Message, sqsMaxMessages)
	go pollQueue(inMsgChan, &inputQueue)
	for message := range inMsgChan {
		user := message.MessageAttributes["User"].StringValue
		// command := message.MessageAttributes["Command"].StringValue
		session := message.Attributes["MessageGroupId"]
		timestamp := message.Attributes["SentTimestamp"]
		// found := searchMessages(user, message.Body)

		log.Infof(
		"%s message: Body='%s', User='%s', Session='%s', Timestamp='%v'\n",
		Green("Received"), Blue(*message.Body), Blue(*user), Blue(*session), Blue(*timestamp))
		// sendMessage(message.Body, user, session, &outputQueue, command)
		log.Infof(
		"%s message: Body='%s', User='%s', Session='%s', Timestamp='%s'\n",
		Yellow("Echoed"), Blue(*message.Body), Blue(*user), Blue(*session), Blue(*timestamp))
			
		// deleteMessage(message.ReceiptHandle, &inputQueue)
	
		log.Infof(
		"%s message: Body='%s', User='%s', Session='%s', Timestamp='%s'\n",
		Red("Deleted"), Blue(*message.Body), Blue(*user), Blue(*session), Blue(*timestamp))
	}
}

func pollQueue(chn chan<- *sqs.Message, queue *string) {
	for {
    output, err := sqsService.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames:					aws.StringSlice([]string{"SentTimestamp","MessageGroupId"}),
      QueueUrl:            		queue,
      MaxNumberOfMessages: 		aws.Int64(sqsMaxMessages),
			WaitTimeSeconds:     		aws.Int64(sqsPollWaitSeconds),
			MessageAttributeNames:	aws.StringSlice([]string{"User", "Command"}),//,"Session", "SentTimestamp"}),
    })

    if err != nil {
      log.Errorf("Failed to fetch sqs message %v", err)
    }

    for _, message := range output.Messages {
			if (*message.MessageAttributes["Command"].StringValue == "search") {
				chn <- message
			} else {
				log.Warnf("Echo system cant handle this request, waiting until timeout")
			}
    }
  }
}
func searchMessages(user *string, search string)(found string) {
	var ret string
	params := &s3.ListObjectsInput {
    Bucket: aws.String(bucket),
    Prefix: aws.String(fmt.Sprintf("%s", *user)),
	}
	resp, _ := s3Service.ListObjects(params)
	// var response string
	for _, object := range resp.Contents {
		buff := &aws.WriteAtBuffer{}
		_, err := downloader.Download(buff, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    object.Key,
		})
		if err != nil {
			log.Errorf("Error downloading S3 object: %v", err)
		}
		buffString := string(buff.Bytes())
		splitBreak := func(c rune) bool {
			return c == '\n'
		}
		splitColon:= func(c rune) bool {
			return c == ':'
		}
		for _, str := range strings.FieldsFunc(buffString, splitBreak) {
			if (len(strings.FieldsFunc(str, splitColon)) > 1) {
				if(strings.Contains(strings.FieldsFunc(str, splitColon)[1], search)) {
					ret = fmt.Sprintf("%s\n%s", ret, str)
				}
			}
		}
	}
	return ret
}
func getQueueUrlByTag(tag string, tagValue string)(string, error) {
	result, err := sqsService.ListQueues(nil)
	if err != nil {
		fmt.Println("Error", err)
		return "", err
	}
	for _, url := range result.QueueUrls {
		if url == nil {
		  continue
		}
		queue := &sqs.ListQueueTagsInput{
    	QueueUrl: url,
		}
		tags, err := sqsService.ListQueueTags(queue)
		if url == nil {
		  return "", err
		}
		// fmt.Println(tags)
		if (*tags.Tags[tag] == tagValue) {
			return *url, nil
		}
	}
	return "", fmt.Errorf("Cant find queue with tag `%s = %s`", tag, tagValue)
}
