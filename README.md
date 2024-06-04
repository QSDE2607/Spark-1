<h1> Phân tích hành vi và thói quen của người dùng Stack Overflow. </h1>

<h2> Tổng quan dự án </h2>

Ở bài Assignment này, bạn sẽ được xây dựng một hệ thống Big Data từ một tập dữ liệu có sẵn. Sản phẩm nộp sẽ cần đảm bảo các yêu cầu sau:

<ol>
<li>mport dữ liệu từ dạng file CSV sang MongoDB.</li>
<li>Sử dụng Spark và đọc dữ liệu từ MongoDB.</li>
<li>Hoàn thành được các yêu cầu của đề bài.</li>
</ol>

<h2> Yêu cầu chi tiết </h2>

<h3> 1. Đưa dữ liệu vào MongoDB </h3>

Bạn sẽ cần chuyển dữ liệu từ dạng file csv thành các Collection trong MongoDB. Để hoàn thành được yêu cầu này, bạn có thể tham khảo câu lệnh sau:

mongoimport --type csv -d <database> -c <collection> --headerline --drop <file>

Bạn sẽ cần thực hiện đoạn code này với cả 2 file Questions.cvs và Answers.csv. Bạn cần thay <database> với tên của Database muốn tạo và <collection> với tên của Collection sẽ chứa dữ liệu.

<h3> 2. Đọc dữ liệu từ MongoDB với Spark </h3>

Sau khi đã đưa dữ liệu từ file csv về dạng Collection ở MongoDB thì bạn sẽ đến bước đọc các dữ liệu đó dưới dạng một Dataframe.

<h3> 3. Chuẩn hóa dữ liệu</h3>

Do dữ liệu ở MongoDB được import từ csv nên các trường như CreationDate, ClosedDate sẽ được lưu dưới dạng String chứ không phải Datetime nên bạn sẽ cần chuyển về kiểu dữ liệu DateType(), hoặc có một số giá trị trong trường OwnerUserId có giá trị là "NA", bạn cũng sẽ phải chuyển các giá trị "NA" về null và để kiểu dữ liệu là IntegerType(). Sau khi đọc dữ liệu từ Question thì Dataframe sẽ có Schema như sau:


root
 |-- Id: integer (nullable = true)
 |-- OwnerUserId: integer (nullable = true)
 |-- CreationDate: date (nullable = true)
 |-- ClosedDate: date (nullable = true)
 |-- Score: integer (nullable = true)
 |-- Title: string (nullable = true)
 |-- Body: string (nullable = true)
<h3> 4. Yêu cầu 1: Tính số lần xuất hiện của các ngôn ngữ lập trình </h3> 

Với yêu cầu này, bạn sẽ cần đếm số lần mà các ngôn ngữ lập trình xuất hiện trong nội dung của các câu hỏi. Các ngôn ngữ lập trình cần kiểm tra là:


Java, Python, C++, C#, Go, Ruby, Javascript, PHP, HTML, CSS, SQL
Để hoàn thành yêu cầu này, bạn có thể sử dụng regex để trích xuất các ngôn ngữ lập trình đã xuất hiện trong từng câu hỏi. Sau đó sử dụng các phép Aggregation để tính tổng theo từng ngôn ngữ. Kết quả sẽ như sau:

![image](https://github.com/QSDE2607/Spark-1/assets/171625181/1d8f13e5-bd6e-452b-95bf-5a1eea5e7547)



<h3> 5. Yêu cầu 2 : Tìm các domain được sử dụng nhiều nhất trong các câu hỏi </h3>

Trong các câu hỏi thường chúng ta sẽ dẫn link từ các trang web khác vào. Ở yêu cầu này, bạn sẽ cần tìm xem 20 domain nào được người dùng sử dụng nhiều nhất. Chú ý rằng các domain sẽ chỉ gồm tên domain, các bạn sẽ không cần trích xuất những tham số phía sau. Ví dụ về một domain: www.google.com, www.facebook.com,...

Để hoàn thành được yêu cầu này, bạn có thể sử dụng regex để trích xuất các url, sau đó áp dung một số biện pháp xử lý chuỗi để lấy ra được tên của domain, cuối cùng là dùng Aggregation để gộp nhóm lại. Kết quả sẽ như sau:

![image](https://github.com/QSDE2607/Spark-1/assets/171625181/4d473fd0-7eff-4d2c-b899-f6299e172283)

<h3> 6. Yêu cầu 3 : Tính tổng điểm của User theo từng ngày</h3>

Bạn cần biết được xem đến ngày nào đó thì User đạt được bao nhiêu điểm. Ví dụ với dữ liệu như sau:

![image](https://github.com/QSDE2607/Spark-1/assets/171625181/034edc12-674e-47fc-8d36-498f6c604c70)

Thì bạn sẽ có được kết quả:

![image](https://github.com/QSDE2607/Spark-1/assets/171625181/70c8fc4d-e481-4f16-9351-f17d71115234)

Để hoàn thành yêu cầu này, bạn sẽ cần sử dụng các thao tác Windowing và các thao tác Aggregation, bạn có thể tham khảo bài Bài 9 : Data Aggregations và Join trên Spark. Kết quả sẽ cần được sắp xếp theo trường OwnerUserId và CreationDate.

<h3> 7. Yêu cầu 4: Tính tổng số điểm mà User đạt được trong một khoảng thời gian </h3>

Ở yêu cầu này, bạn sẽ cần tính tổng điểm mà User đạt được khi đặt câu hỏi trong một khoảng thời gian. Ví dụ như bạn muốn tính xem từ ngày 01-01-2008 đến 01-01-2009 thì các user đạt được bao nhiêu điểm từ việc đặt câu hỏi. Các khoảng thời gian này sẽ được khai báo trực tiếp trong code, ví dụ như sau:


START = '01-01-2008'
END = '01-01-2009'

if __name__ == '__main__':
    pass

Để hoàn thành yêu cầu này, bạn sẽ cần sử dụng filter() để lọc ra các dữ liệu thỏa mãn từ khung dữ liệu, sau đó có thể làm theo yêu cầu 4. Kết quả sẽ cần được sắp xếp theo trường OwnerUserId, ví dụ:

![image](https://github.com/QSDE2607/Spark-1/assets/171625181/6b20d7a9-20b4-454c-b33b-bc7aeaf68ea7)


<h3> 8. Yêu cầu 5: Tìm các câu hỏi có nhiều câu trả lời </h3>

Một câu hỏi tốt sẽ được tính số lượng câu trả lời của câu hỏi đó, nếu như câu hỏi có nhiều hơn 5 câu trả lời thì sẽ được tính là tốt. Bạn sẽ cần tìm xem có bao nhiêu câu hỏi đang được tính là tốt,  

Để hoàn thành yêu cầu này, bạn sẽ cần sử dụng các thao tác Join để gộp dữ liệu từ Answers với Collections, sau đó dụng các thao tác Aggregation để gộp nhóm, tính xem mỗi câu hỏi đã có bao nhiêu câu trả lời. Cuối cùng là dùng hàm filter() để lọc ra các câu hỏi có nhiều hơn 5 câu trả lời. 

Lưu ý: Do thao tác có thể tốn rất nhiều thời gian, nên bạn hãy sử dụng cơ chế Bucket Join để phân vùng cho các dữ liệu trước. Bạn có thể tham khảo Bài 9 : Data Aggregations và Join để hiểu rõ hơn về cơ chế này.

Kết quả sẽ cần được sắp xếp theo ID của các câu hỏi

<h3> 9. (Nâng cao) Yêu cầu 6: Tìm các Active User </h3>

Một User được tính là Active sẽ cần thỏa mãn một trong các yêu cầu sau:

Có nhiều hơn 50 câu trả lời hoặc tổng số điểm đạt được khi trả lời lớn hơn 500.
Có nhiều hơn 5 câu trả lời ngay trong ngày câu hỏi được tạo.
Bạn hãy lọc các User thỏa mãn điều kiện trên.
