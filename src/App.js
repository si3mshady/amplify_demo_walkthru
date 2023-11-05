import React, { useState, useEffect } from "react";
import Webcam from "react-webcam";
import "./App.css";
import { Amplify, API} from 'aws-amplify';
import awsmobile from './aws-exports';

Amplify.configure(
  awsmobile
);



const App = () => {
  const [images, setImages] = useState([]);
  const [location, setLocation] = useState(null);
  const [labels, setLabels] = useState([]);
  const [image, setImage] = useState(null);

  const [isCapturing, setIsCapturing] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [data, setPostData] = useState({body: {}, headers: {}});
  const [facingMode, setFacingMode] = useState("user"); 


  const [resp, setResp] = React.useState("")

  
  
  const captureNew = async () => {
    setIsCapturing(true); // Set isCapturing to true while capturing

    const screenshot = webcamRef.current.getScreenshot();

    if (screenshot) {
      setImage(screenshot);

    
      navigator.geolocation.getCurrentPosition((position) => {
        setLocation({
          latitude: position.coords.latitude ,
          longitude: position.coords.longitude,
        });



      });
    } else {
      console.log("Invalid image format. Please capture a PNG or JPEG image.");
    }

    setIsCapturing(false); // Reset isCapturing when done capturing
  };

  const handleUploadNew = () => {
    console.log("sending data")
    setIsUploading(true); // Set isUploading to true while uploading

     console.log( {base64Data: image});
    const data = JSON.stringify({base64Data: image, coordinates: JSON.stringify(location)})


    // const b64data = JSON.stringify(screenshot)
    // const apiName = 'doggonepy3'; // replace this with your api name.
    // const path = '/upload';

    // API.post(apiName, path, myInit)
    // .then((response) => {
    //   // Add your code here
    //   console.log(response)
    //   console.log("Data sent")
    //   setIsUploading(false); // Set
    // })
    // .catch((error) => {
    //   console.log(error.response);
    // });



    
    fetch('https://6x5v89cjs6.execute-api.us-east-1.amazonaws.com/dev/upload', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
  })
  .then((response) => response.json())
  .then((data) => {
  if (data) {
    console.log( data)

      setLabels(data.labels.Labels)
   

      console.log( data.labels)
  } else {
    console.error('Upload failed: ' + data.message);
  }})
  .catch((error) => {
    console.error('An error occurred:', error);});

    setIsUploading(false); // Reset isUploading when done uploading
  };


  const webcamRef = React.useRef(null);


  // Function to toggle between the front and back camera
  const toggleCamera = () => {
    setFacingMode((prevMode) => (prevMode === "user" ? "environment" : "user"));
  };


  return (
    <div className="app-container">
    <h3>Image </h3>
    <h3>Classification </h3>
    <div className="container">
      <div className="webcam-container">
      <Webcam
            audio={false}
            screenshotFormat="image/jpeg"
            height={200}
            width={200}
            ref={webcamRef}
            videoConstraints={{
              facingMode,
            }}
          />

        <button onClick={captureNew} disabled={isCapturing || isUploading}>
          Capture
        </button>
        <button onClick={handleUploadNew} disabled={isCapturing || isUploading}>
          Upload
        </button>
        <button onClick={toggleCamera} disabled={isCapturing || isUploading}>
            Toggle Camera
          </button>
      </div>
    </div>

        <div className="image-container">
            {/* Display the last captured image */}
            {/* {isCapturing ? (
          // Show a spinner while capturing
          <div className="spinner"></div>
        ) : (
          // Display the last captured image when not capturing
          image && (
            <div className="image-card">
              <img src={image} alt={`Last Captured`} />
            </div>
          )
        )} */}


            {labels.length > 0 && (
          <div className="labels">
            <h2>Labels:</h2>
            <ul>
              {labels.map((label, index) => (
                <li key={index}>{label.Name}</li>
              ))}
            </ul>
          </div>
        )}
            {/* ... (rest of your code for displaying analysis results) */}
        </div>
        {location && (
          <div className="gps">
          <p>
                {/* Latitude: {location.latitude} */}
            </p>

            <p>
             {/* Longitude: {location.longitude} */}
            </p>
          
          </div>
           
            
        )}
       
    </div>
  );
};

export default App;

